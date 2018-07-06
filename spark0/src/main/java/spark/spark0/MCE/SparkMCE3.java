package spark.spark0.MCE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import spark.spark0.Graph;

/**
 * @author fxf
 * 
 *         在spark上实现极大团挖掘算法，使用了TTT算法，并且可以计算重叠社区
 * 
 *         使用广播变量，将图数据发送到各个节点上
 * 
 *         本地eclipse计算
 *
 */
public class SparkMCE3 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		long sTime = System.currentTimeMillis();

		// spark配置对象
		SparkConf conf = new SparkConf().setAppName("MCE-spark").setMaster("local");
		// SparkContext对象
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 从file中读取，创建broadcast变量
		File file = new File("E:/JavaProject/graph_data/graph2_10_format.txt");
		BufferedReader reader = null;
		Graph graph = new Graph();
		ArrayList<ArrayList<String>> adjGraph = new ArrayList<>();
		try {
			reader = new BufferedReader(new FileReader(file));
			String str = null;

			while ((str = reader.readLine()) != null) {
				ArrayList<String> line = new ArrayList<>(Arrays.asList(str.split(" ")));
				// System.out.println(line);
				adjGraph.add(line);
				graph.addNodes(line.get(0));
				if (line.size() > 1) {
					graph.addEdges(line.get(0), new ArrayList<>(line.subList(1, line.size())));
				} else {
					graph.addEdges(line.get(0), new ArrayList<>());
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// 不从文件中读取，生成随机图数据
		// RandomGraph randomGraph = new RandomGraph();
		// Graph graph = randomGraph.generateUndirectedGraph(50); // 生成随机数
		// graph.printGraph();
		// ArrayList<ArrayList<String>> adjGraph = new
		// ArrayList<>(randomGraph.generateAdjGraph());

		// // 将生成的图数据写入文件
		// File file3 = new File("./data_50.txt");
		// try {
		// BufferedWriter bw = new BufferedWriter(new FileWriter(file3));
		// for (int i = 0; i < graph.getNodes().size(); i++) {
		// bw.write(graph.getNode(i) + " ");
		// ArrayList<String> nEdges = graph.getEdges(graph.getNode(i));
		// for (int j = 0; j < nEdges.size(); j++) {
		// bw.write(nEdges.get(j) + " ");
		// }
		// bw.flush();
		// bw.newLine();
		// }
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// System.out.println("adjGraph");
		//
		// for (ArrayList<String> edges : adjGraph) {
		// System.out.println(edges.toString());
		// }
		System.out.println("readover");
		long rTime = System.currentTimeMillis();

		JavaRDD<ArrayList<String>> graph_nodes = sc.parallelize(adjGraph);
		// 第二个参数为分区的个数，分的多，每个分区的数据量会减少

		// 使用广播方法，将图数据的信息分发到每一个节点上
		final Broadcast<Graph> graph_bc = sc.broadcast(graph);

		System.out.println("startMCE");
		// ************MCE，级大团挖掘开始

		JavaRDD<ArrayList<String>> cliques = graph_nodes
				.flatMap(new FlatMapFunction<ArrayList<String>, ArrayList<String>>() {

					@Override
					public Iterator<ArrayList<String>> call(ArrayList<String> nodes) throws Exception {
						// TODO Auto-generated method stub
						// 用TTT算法进行极大团挖掘

						// SparkTTT2 tttAlgth = new SparkTTT2(graph_bc.value(), nodes);
						// // 创建SparkTTT2类的对象
						SparkTTT3 tttAlgth = new SparkTTT3(graph_bc.value(), nodes);
						// 创建SparkTTT3类的对象
						ArrayList<ArrayList<String>> cs = tttAlgth.TTTMCE();// 调用函数，nodes中的极大团
						return cs.iterator();
					}
				});
		// 测试spark对=，和for循环的支持
		/*
		 * JavaRDD<ArrayList<String>> cliques2 = cliques; for (int i = 0; i < 5; i++) {
		 * System.out.println(i); cliques.foreach(new VoidFunction<ArrayList<String>>()
		 * {
		 * 
		 * @Override public void call(ArrayList<String> t) throws Exception { // TODO
		 * Auto-generated method stub System.out.println("cliques:" + t.toString()); }
		 * }); cliques = cliques2; }
		 */

		// 将计算出的cliques按照长度排序
		JavaPairRDD<Integer, ArrayList<String>> pairCliques = cliques
				.mapToPair(new PairFunction<ArrayList<String>, Integer, ArrayList<String>>() {

					@Override
					public Tuple2<Integer, ArrayList<String>> call(ArrayList<String> t) throws Exception {
						// TODO Auto-generated method stub
						Tuple2<Integer, ArrayList<String>> newClique = new Tuple2<Integer, ArrayList<String>>(t.size(),
								t);
						return newClique;
					}
				});

		JavaPairRDD<Integer, ArrayList<String>> sortedPairCliques = pairCliques.sortByKey();
		JavaRDD<ArrayList<String>> sortedCliques = sortedPairCliques
				.map(new Function<Tuple2<Integer, ArrayList<String>>, ArrayList<String>>() {

					@Override
					public ArrayList<String> call(Tuple2<Integer, ArrayList<String>> v1) throws Exception {
						// TODO Auto-generated method stub
						return v1._2;
					}
				});

		ArrayList<ArrayList<String>> cliques_list = new ArrayList<>(sortedCliques.collect());

		long mceTime = System.currentTimeMillis();
		File file2 = new File("E:/JavaProject/graph_data/graph2_10_cliques.txt");
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file2));
			for (int i = 0; i < cliques_list.size(); i++) {
				System.out.println("cliques:" + cliques_list.get(i).toString());
				bw.write(cliques_list.get(i).toString());
				bw.flush();
				bw.newLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		long wTime = System.currentTimeMillis();

		System.out.println("读文件时间:" + (rTime - sTime) / 1000 + "s");
		System.out.println("MCE算法计算时间：" + (mceTime - rTime) / 1000 + "s");
		System.out.println("写文件时间：" + (wTime - mceTime) / 1000 + "s");

	}
}
