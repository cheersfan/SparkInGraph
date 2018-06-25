package spark.spark0.MCE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * @author fxf
 * 
 *         在spark上实现极大团挖掘算法，使用了TTT算法，并且可以计算重叠社区
 * 
 *         使用collect获取全部的图信息，而非使用广播变量(测试后失败)
 * 
 *         对SparkMCE2重叠社区方法的优化，进行笛卡儿积之后，用fliter进行过滤之后，再将同一个团的重叠社区汇总到一起，减少冗余计算
 * 
 *         计算重叠社区方法： 对JavaRDD类型的cliques自己对自己进行笛卡儿积，再 进行极大团的计算
 * 
 *         本地eclipse计算
 *
 */
public class SparkMCE2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// spark配置对象
		SparkConf conf = new SparkConf().setAppName("MCE-spark").setMaster("local");
		// SparkContext对象
		JavaSparkContext sc = new JavaSparkContext(conf);

		/*
		 * // 从file中读取，创建broadcast变量 File file = new File("data4.txt"); BufferedReader
		 * reader = null; ArrayList<ArrayList<String>> graph = new ArrayList<>(); try {
		 * reader = new BufferedReader(new FileReader(file)); String str = null;
		 * 
		 * while ((str = reader.readLine()) != null) { graph.add(new
		 * ArrayList<>(Arrays.asList(str.split(" ")))); } } catch (Exception e) { //
		 * TODO Auto-generated catch block e.printStackTrace(); }
		 */

		System.out.println("readover");

		// 读取数据 file->String
		JavaRDD<String> graph_file = sc.textFile("data4.txt");
		// String->{"1 2 9","2 1 3 9",...}
		JavaRDD<String> graph_str = graph_file.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(t.split("/n")).iterator();
			}
		});

		// {"1 2 9","2 1 3 9",...}->{"1","2","9"},{"2","1","3","9"}...
		JavaRDD<ArrayList<String>> graph_nodes = graph_str.map(new Function<String, ArrayList<String>>() {

			@Override
			public ArrayList<String> call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return new ArrayList<>(Arrays.asList(v1.split(" ")));
			}
		});

		ArrayList<ArrayList<String>> graph = new ArrayList<>(graph_nodes.collect());
		System.out.println("startMCE");
		// ************MCE，级大团挖掘开始
		JavaRDD<ArrayList<String>> cliques = graph_nodes
				.flatMap(new FlatMapFunction<ArrayList<String>, ArrayList<String>>() {

					@Override
					public Iterator<ArrayList<String>> call(ArrayList<String> nodes) throws Exception {
						// TODO Auto-generated method stub
						// 用TTT算法进行极大团挖掘

						SparkTTT tttAlgth = new SparkTTT(graph, nodes);// 创建SparkTTT2类的对象
						ArrayList<ArrayList<String>> cs = tttAlgth.TTTMCE();// 调用函数，nodes中的极大团
						return cs.iterator();
					}
				});

		cliques.foreach(new VoidFunction<ArrayList<String>>() {

			@Override
			public void call(ArrayList<String> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("cliques:" + t.toString());
			}
		});

		// 团的重叠社区检测
		// 使用笛卡儿积
		JavaPairRDD<ArrayList<String>, ArrayList<String>> cliques_carts = cliques.cartesian(cliques);
		// 针对两个集合，判断是否为重叠社区
		JavaPairRDD<ArrayList<String>, ArrayList<String>> cliques_carts_comm = cliques_carts.mapToPair(
				new PairFunction<Tuple2<ArrayList<String>, ArrayList<String>>, ArrayList<String>, ArrayList<String>>() {

					@Override
					public Tuple2<ArrayList<String>, ArrayList<String>> call(
							Tuple2<ArrayList<String>, ArrayList<String>> t) throws Exception {
						// TODO Auto-generated method stub
						ArrayList<String> q = new ArrayList<>(t._2);
						ArrayList<String> tt = new ArrayList<>(t._2);
						q.retainAll(t._1);
						// 重叠社区的阈值为2，两个团的相同定点个数少于2时，不为重叠社区，并将两个完全相同的团也判定为非重叠社区
						if (q.size() < 2 || q.equals(t._1)) {
							tt = null;
						}
						return new Tuple2<ArrayList<String>, ArrayList<String>>(t._1, tt);
					}
				});

		// 对所求得的重叠社区进行过滤
		JavaPairRDD<ArrayList<String>, ArrayList<String>> cliques_carts_comms2 = cliques_carts_comm
				.filter(new Function<Tuple2<ArrayList<String>, ArrayList<String>>, Boolean>() {

					@Override
					public Boolean call(Tuple2<ArrayList<String>, ArrayList<String>> v1) throws Exception {
						// TODO Auto-generated method stub

						boolean flag = false;

						if (v1._2 != null) {
							flag = true;
						}
						return flag;
					}
				});

		cliques_carts_comms2.foreach(new VoidFunction<Tuple2<ArrayList<String>, ArrayList<String>>>() {

			@Override
			public void call(Tuple2<ArrayList<String>, ArrayList<String>> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("cartesion_community:" + t._1.toString() + " " + t._2.toString());
			}
		});
		// 转换一下所求的笛卡儿积的JavaPairRDD的类型
		JavaPairRDD<ArrayList<String>, ArrayList<ArrayList<String>>> cliques_carts2 = cliques_carts_comms2.mapToPair(
				new PairFunction<Tuple2<ArrayList<String>, ArrayList<String>>, ArrayList<String>, ArrayList<ArrayList<String>>>() {

					@Override
					public Tuple2<ArrayList<String>, ArrayList<ArrayList<String>>> call(
							Tuple2<ArrayList<String>, ArrayList<String>> t) throws Exception {
						// TODO Auto-generated method stub
						ArrayList<ArrayList<String>> list = new ArrayList<>();
						list.add(t._2);
						return new Tuple2<ArrayList<String>, ArrayList<ArrayList<String>>>(t._1, list);
					}
				});

		// 将笛卡儿积的JavaPairRDD对象进行规约，将相同key值的v合并到一个ArrayList<ArrayList<String>>对象中
		JavaPairRDD<ArrayList<String>, ArrayList<ArrayList<String>>> communities = cliques_carts2.reduceByKey(
				new Function2<ArrayList<ArrayList<String>>, ArrayList<ArrayList<String>>, ArrayList<ArrayList<String>>>() {

					@Override
					public ArrayList<ArrayList<String>> call(ArrayList<ArrayList<String>> v1,
							ArrayList<ArrayList<String>> v2) throws Exception {
						// TODO Auto-generated method stub
						v1.add(v2.get(0));
						return v1;
					}
				});

		// 输出重叠社区
		communities.foreach(new VoidFunction<Tuple2<ArrayList<String>, ArrayList<ArrayList<String>>>>() {

			@Override
			public void call(Tuple2<ArrayList<String>, ArrayList<ArrayList<String>>> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("1:" + t._1.toString() + " 2:" + t._2.toString());
			}
		});

		// 输出极大团
		cliques.foreach(new VoidFunction<ArrayList<String>>() {

			@Override
			public void call(ArrayList<String> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("spark");
				System.out.println(t.toString());

			}
		});
	}
}
