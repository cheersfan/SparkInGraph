package spark.spark0.kcommunity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * @author fxf Computing overlapping communities 计算重叠社区
 *         将全部的团广播出去，然后遍历团数据的每个团，计算重叠矩阵，继而计算重叠社区 （注意重叠社区的定义） *
 */
public class SparkCOC1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		long startTime = System.currentTimeMillis();
		String cliquesPath = "E:/JavaProject/graph_data/graph_10_cliques.txt";
		String outputPath = "E:/JavaProject/graph_data/graph_10_comm.txt";

		// 创建clique工具对象
		CliqueUtil cliqueUtil = new CliqueUtil();
		// 从文件file中读取cliques数据，创建cliques对象，cliques包含了全部的团数据
		ArrayList<Clique> myCliques = new ArrayList<>(cliqueUtil.getCliquesFromPath(cliquesPath));

		// for (Clique clique : myCliques) {
		// System.out.println("id:" + clique.cliqueID + " clique:" + clique.cliqueInfo);
		// }

		// Spark配置对象
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Spack_COC");
		// SparkContext对象
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 广播变量
		final Broadcast<ArrayList<Clique>> cliques_bc = sc.broadcast(myCliques);

		// 创建RDD对象
		JavaRDD<Clique> myCliquesRDD = sc.parallelize(myCliques);

		myCliquesRDD.foreach(new VoidFunction<Clique>() {

			@Override
			public void call(Clique t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("id:" + t.cliqueID + " clique:" + t.cliqueInfo);
			}
		});

		// 计算重叠矩阵->连接矩阵
		JavaPairRDD<Clique, Clique> myOLMatrixPairRDD = myCliquesRDD
				.flatMapToPair(new PairFlatMapFunction<Clique, Clique, Clique>() {

					@Override
					public Iterator<Tuple2<Clique, Clique>> call(Clique c1) throws Exception {
						// TODO Auto-generated method stub
						ArrayList<Clique> allCliques = new ArrayList<>(cliques_bc.value());// 获取广播变量的值
						ArrayList<Tuple2<Clique, Clique>> matrix = new ArrayList<>();
						for (Clique c2 : myCliques) {
							if (c1.cliqueID <= c2.cliqueID) {// 只计算矩阵的上三角
								int myThreshold = 4;// 设置阈值
								int olNum = cliqueUtil.getOLNum(c1, c2);// 两个团的重叠度
								if (c1.cliqueID == c2.cliqueID && olNum >= myThreshold) {// 矩阵对角线上，小于阈值，该值（重叠度）置为0;大于或等于阈值，置为1
									olNum = 1;
								} else if (c1.cliqueID != c2.cliqueID && olNum >= myThreshold - 1) {// 矩阵非对角线上，小于阈值-1，该值（重叠度）置为0;大于或等于阈值-1，置为1
									olNum = 1;
								} else {
									olNum = 0;
								}
								if (olNum > 0) {
									int pid = (c1.parentID <= c2.parentID) ? c1.parentID : c2.parentID;
									c1.setParentID(pid);
									c2.setParentID(pid);
									matrix.add(new Tuple2<Clique, Clique>(c1, c2));// Matrix只记录了，只存储了值为1的情况
								}
							}
						}
						return matrix.iterator();
					}
				});

		// 输出测数据
		myOLMatrixPairRDD.foreach(new VoidFunction<Tuple2<Clique, Clique>>() {

			@Override
			public void call(Tuple2<Clique, Clique> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("id1:" + t._1.cliqueID + " id1_parent:" + t._1.parentID + " id2:" + t._2.cliqueID
						+ " id2_parent:" + t._2.parentID);
			}
		});

		// 将id1相同的团聚合到一起
		JavaPairRDD<Clique, Iterable<Clique>> myOLMatrixPairRDD2 = myOLMatrixPairRDD.groupByKey();

		// 聚合成一团的parentID为parentID小的团的值，修改聚合到一起的团的parentID
		JavaPairRDD<Integer, ArrayList<Clique>> myOLMatrixPairRDD3 = myOLMatrixPairRDD2
				.mapToPair(new PairFunction<Tuple2<Clique, Iterable<Clique>>, Integer, ArrayList<Clique>>() {

					@Override
					public Tuple2<Integer, ArrayList<Clique>> call(Tuple2<Clique, Iterable<Clique>> t)
							throws Exception {
						// TODO Auto-generated method stub
						int pid = t._1.parentID;
						ArrayList<Clique> cliques = new ArrayList<>();// parentID的部分团
						Iterator<Clique> cIterator = t._2.iterator();
						while (cIterator.hasNext()) {// 用最小的parenID作为每个团的parentID
							Clique clique = new Clique(cIterator.next());
							pid = (clique.parentID < pid) ? clique.parentID : pid;
						}

						cIterator = t._2.iterator();
						while (cIterator.hasNext()) {
							Clique clique = new Clique(cIterator.next());
							clique.setParentID(pid);
							cliques.add(clique);

						}
						return new Tuple2<Integer, ArrayList<Clique>>(pid, cliques);
					}
				});
		// 输出测试数据
		myOLMatrixPairRDD3.foreach(new VoidFunction<Tuple2<Integer, ArrayList<Clique>>>() {

			@Override
			public void call(Tuple2<Integer, ArrayList<Clique>> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("pid:" + t._1);
				Iterator<Clique> cIterator = t._2.iterator();

				while (cIterator.hasNext()) {
					Clique clique = new Clique(cIterator.next());
					System.out.print("cid:" + clique.cliqueID + " pid:" + clique.parentID + " ");
				}
				System.out.println();
			}
		});

		// 合并求得最终的社区
		JavaPairRDD<Integer, ArrayList<Clique>> myComm = myOLMatrixPairRDD3
				.reduceByKey(new Function2<ArrayList<Clique>, ArrayList<Clique>, ArrayList<Clique>>() {

					@Override
					public ArrayList<Clique> call(ArrayList<Clique> cliques1, ArrayList<Clique> cliques2)
							throws Exception {
						// TODO Auto-generated method stub
						for (Clique clique : cliques2) {
							if (!cliques1.contains(clique)) {
								cliques1.add(clique);
							}
						}
						return cliques1;
					}
				});

		// 输出求得的社区数据
		myComm.foreach(new VoidFunction<Tuple2<Integer, ArrayList<Clique>>>() {

			@Override
			public void call(Tuple2<Integer, ArrayList<Clique>> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.print("pid:" + t._1);
				for (Clique clique : t._2) {
					System.out.print(" cid:" + clique.cliqueID);
				}
				System.out.println();
			}
		});

		// 将求得的社区数据汇总到一起
		HashMap<Integer, ArrayList<Clique>> myCommMap = new HashMap<>(myComm.collectAsMap());
		// 写文件
		cliqueUtil.writeCommToPath(outputPath, myCommMap);
	}

}
