package spark.spark0.MCE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * @author fxf
 * 
 *         在spark上实现极大团挖掘算法，使用了TTT算法，并且可以计算重叠社区 计算重叠社区的方法：
 *         新定义一个广播变量，里面存储了已挖掘出的全部的团，再对JavaRDD对象cliques里的每一个团来计算重叠社区
 *
 */
public class SparkMCE0 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// spark配置对象
		SparkConf conf = new SparkConf().setAppName("MCE-spark").setMaster("local");
		// SparkContext对象
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 从file中读取，创建broadcast变量
		File file = new File("data3.txt");
		BufferedReader reader = null;
		ArrayList<ArrayList<String>> graph = new ArrayList<>();
		try {
			reader = new BufferedReader(new FileReader(file));
			String str = null;

			while ((str = reader.readLine()) != null) {
				graph.add(new ArrayList<>(Arrays.asList(str.split(" "))));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// 使用广播方法，将图数据的信息分发到每一个节点上
		final Broadcast<ArrayList<ArrayList<String>>> graph_bc = sc.broadcast(graph);

		// 读取数据 file->String
		JavaRDD<String> graph_file = sc.textFile("data3.txt");
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

		// MCE，级大团挖掘开始
		JavaRDD<ArrayList<String>> cliques = graph_nodes
				.flatMap(new FlatMapFunction<ArrayList<String>, ArrayList<String>>() {

					@Override
					public Iterator<ArrayList<String>> call(ArrayList<String> nodes) throws Exception {
						// TODO Auto-generated method stub
						// 用TTT算法进行极大团挖掘
						SparkTTT tttAlgth = new SparkTTT(graph_bc.value(), nodes);
						ArrayList<ArrayList<String>> cs = tttAlgth.TTTMCE();
						return cs.iterator();
					}
				});

		// 团的相似判断
		// 使用广播变量，将求得的团数据分发到每一个节点上
		ArrayList<ArrayList<String>> allCliques = new ArrayList<>(cliques.collect());
		final Broadcast<ArrayList<ArrayList<String>>> cliques_bc = sc.broadcast(allCliques);

		JavaPairRDD<ArrayList<String>, ArrayList<ArrayList<String>>> communities = cliques
				.mapToPair(new PairFunction<ArrayList<String>, ArrayList<String>, ArrayList<ArrayList<String>>>() {

					@Override
					// Tuple2<T1, T2>,K:T1,V:T2
					public Tuple2<ArrayList<String>, ArrayList<ArrayList<String>>> call(ArrayList<String> t)
							throws Exception {
						// TODO Auto-generated method stub
						// 与团t重叠的团 simi
						ArrayList<ArrayList<String>> simis = new ArrayList<>();

						// 团t与其他全部的团求交集，可以用偏序方法去除冗余计算
						for (ArrayList<String> clique : cliques_bc.value()) {
							// 如果团t的第一个节点 小于等于 求交的团第一个节点，且两个团不想等，才进行求交（使用偏虚方法去除冗余--有问题！！）
							// if ((Integer.parseInt(t.get(0)) <= Integer.parseInt(clique.get(0))){
							// ) {
							if (!(t.equals(clique))) {
								ArrayList<String> ans = new ArrayList<>(t);
								ans.retainAll(clique);
								// System.out.println("ans:" + ans.toString());
								// 设置的阈值为3，如果相似点大于等于3，则判断两个团相似，为重叠社区
								if (ans.size() >= 2) {
									simis.add(clique);
								}
							}
						}
						return new Tuple2<ArrayList<String>, ArrayList<ArrayList<String>>>(t, simis);
					}
				});

		// 输出极大团
		cliques.foreach(new VoidFunction<ArrayList<String>>() {

			@Override
			public void call(ArrayList<String> t) throws Exception {
				// TODO Auto-generated method stub
				// System.out.println("spark");
				// System.out.println(t.toString());

			}
		});

		// 输出重叠社区
		communities.foreach(new VoidFunction<Tuple2<ArrayList<String>, ArrayList<ArrayList<String>>>>() {

			@Override
			public void call(Tuple2<ArrayList<String>, ArrayList<ArrayList<String>>> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("clique:" + t._1.toString());
				System.out.println("communities:" + t._2.toString());
			}
		});
	}
}
