package spark.spark0.MCE;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import spark.spark0.Graph;

/**
 * @author fxf
 *
 */
public class SparkMCE_V2_0 {
	public static void main(String[] args) {
		// 创建sparkConf,sparkContext
		SparkConf conf = new SparkConf().setAppName("sparkMCE_v2").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 文件的读写路径
		String readGraphPath = "E:/JavaProject/graph_data/graph_10_format.txt";
		String writeGraphPath = "E:/JavaProject/graph_data/graph_10_cliques.txt";

		// 1.读取图数据，转换为Graph类，
		Graph graph = new Graph(readGraphPath);
		ArrayList<ArrayList<String>> adjGraph = graph.getAdjGraph();

		// 2.创建广播变量
		Broadcast<Graph> graph_bc = sc.broadcast(graph);

		// 3.将adjGraph数据转换为RDD
		JavaRDD<ArrayList<String>> graphRDD = sc.parallelize(adjGraph);

		graphRDD.foreach(new VoidFunction<ArrayList<String>>() {

			@Override
			public void call(ArrayList<String> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t.toString());
			}
		});

		// 4.调用TTT方法，计算cliques
		JavaRDD<ArrayList<String>> cliques = graphRDD
				.flatMap(new FlatMapFunction<ArrayList<String>, ArrayList<String>>() {
					@Override
					public Iterator<ArrayList<String>> call(ArrayList<String> list) {
						Graph graph_bc_value = graph_bc.value();
						SparkTTT_V2_0 ttt = new SparkTTT_V2_0();
						ArrayList<ArrayList<String>> cliqueList = new ArrayList<>(ttt.TTTMethod(list, graph_bc_value));
						return cliqueList.iterator();
					}
				});

		// 5.将求得的全部级打团,汇集到一起，存入文件中
		ArrayList<ArrayList<String>> clique_list = new ArrayList<>(cliques.collect());

		File writeCliqueFile = new File(writeGraphPath);
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(writeCliqueFile));
			for (ArrayList<String> clique : clique_list) {
				bw.write(clique.toString());
				bw.flush();
				bw.newLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
