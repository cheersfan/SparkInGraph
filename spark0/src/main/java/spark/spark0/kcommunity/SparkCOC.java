package spark.spark0.kcommunity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * @author fxf Computing overlapping communities 计算重叠社区
 *         将全部的团广播出去，然后遍历团数据的每个团，计算重叠社区
 *
 */
public class SparkCOC {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		long startTime = System.currentTimeMillis();
		String cliquesPath = "E:/JavaProject/graph_data/CA-GrQc_5K_cliques.txt";
		String outputPath = "E:/JavaProject/graph_data/CA-GrQc_5K_comm.txt";

		// 从文件file中读取cliques数据，创建cliques对象，已按照团的节点个数排序
		Cliques myCliques = new Cliques(cliquesPath);

		// Spark配置对象
		SparkConf conf = new SparkConf().setAppName("COC-spark").setMaster("local");
		// SparkContext对象
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 团数据 -> JavaRDD，团的数据为Arraylist，存储方式 index + clique
		JavaRDD<ArrayList<String>> cliques_rdd = sc.parallelize(myCliques.getCliquesList()); // cliques_rdd的数据内容index+clique

		long readOver = System.currentTimeMillis();
		// 创建广播变量
		final Broadcast<Cliques> cliques_bc = sc.broadcast(myCliques);// myCliques:为团数据的对象

		JavaRDD<ArrayList<String>> cliquesOverlapNum = cliques_rdd
				.map(new Function<ArrayList<String>, ArrayList<String>>() {

					@Override
					public ArrayList<String> call(ArrayList<String> c) throws Exception {
						// TODO Auto-generated method stub
						// 当前的clique与广播变量里的全部cliques求重叠社区
						String ID1 = c.get(0); // 第一个团的ID
						String ID2 = ""; // 第二个团的ID
						String OLNum = ""; // 两个团的重叠度
						HashMap<Integer, ArrayList<String>> cliques = new HashMap<>(cliques_bc.value().getCliques());// cliques_bc中的数据内容：index+clique
						ArrayList<String> clique_comm = new ArrayList<>();

						for (Entry<Integer, ArrayList<String>> entry : cliques.entrySet()) {
							ID2 = entry.getKey() + ""; // 第二个团的ID
							// System.out.println("ID1:" + ID1 + " ID2:" + ID2);

							if (Integer.parseInt(ID1) < Integer.parseInt(ID2)) {// 计算两个不同团的重叠度
								int count = 0;
								ArrayList<String> clique1 = new ArrayList<>(c.subList(1, c.size()));
								ArrayList<String> clique2 = new ArrayList<>(entry.getValue());
								// System.out.println("clique1:" + clique1.toString() + " clique2:" +
								// clique2.toString());

								for (int i = 0; i < clique1.size(); i++) {
									for (int j = 0; j < clique2.size(); j++) {
										if (clique1.get(i).equals(clique2.get(j))) {
											count += 1;
											// System.out.println("******************" + count);
										}
									}
								}
								OLNum = count + "";
								System.out.println("count:" + count);
								if (count > 0) {
									clique_comm.add(ID1 + " " + ID2 + " " + OLNum);
									System.out.println(ID1 + " " + ID2 + " " + OLNum);
								}
							}

						}
						System.out.println(clique_comm.toString());
						return clique_comm;
					}
				});

		cliquesOverlapNum.foreach(new VoidFunction<ArrayList<String>>() {

			@Override
			public void call(ArrayList<String> cliques) throws Exception {
				// TODO Auto-generated method stub
				for (String clique : cliques) {
					ArrayList<String> list = new ArrayList<>(Arrays.asList(clique.split(" ")));
					// if (Integer.parseInt(list.get(2)) >= 4) {
					System.out.println("list");
					System.out.println(list.toString());
					// }
				}
			}
		});

		long writeOver = System.currentTimeMillis();
		System.out.println("读取时间:" + (readOver - startTime) / 1000 + " s");
		System.out.println("计算时间:" + (writeOver - readOver) / 1000 + " s");

	}

}
