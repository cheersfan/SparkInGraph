package spark.spark0.kcommunity;

import java.util.ArrayList;
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
 * @author fxf Computing overlapping communities 计算重叠社区
 *         将全部的团广播出去，然后遍历团数据的每个团，计算重叠矩阵，继而计算重叠社区 （注意重叠社区的定义）
 *
 */
public class SparkCOC0 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		long startTime = System.currentTimeMillis();
		String cliquesPath = "E:/JavaProject/graph_data/graph_10_cliques.txt";
		String outputPath = "E:/JavaProject/graph_data/graph_10_comm.txt";

		// 从文件file中读取cliques数据，创建cliques对象，cliques包含了全部的团数据
		CliqueUtil cliqueUtil = new CliqueUtil();
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

		// 计算重叠矩阵
		JavaRDD<OLMatrixNode> myOLMatrixRDD = myCliquesRDD.flatMap(new FlatMapFunction<Clique, OLMatrixNode>() {

			@Override
			public Iterator<OLMatrixNode> call(Clique c1) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Clique> allCliques = new ArrayList<>(cliques_bc.value());// 获取广播变量的值
				ArrayList<OLMatrixNode> olMatrixNodeList = new ArrayList<>();
				for (Clique c2 : myCliques) {
					if (c1.cliqueID <= c2.cliqueID) {// 只用计算矩阵的上三角即可
						OLMatrixNode olMatrixNode = new OLMatrixNode(c1, c2, cliqueUtil.getOLNum(c1, c2));
						olMatrixNodeList.add(olMatrixNode);
					}
				}
				return olMatrixNodeList.iterator();
			}
		});

		// 重叠矩阵转换为k派系的连接矩阵
		JavaRDD<OLMatrixNode> myOLConnectMatrixRDD = myOLMatrixRDD.map(new Function<OLMatrixNode, OLMatrixNode>() {

			@Override
			public OLMatrixNode call(OLMatrixNode olMatrixNode) throws Exception {
				// TODO Auto-generated method stub
				int myThreshold = 4;// 设置阈值
				int id1 = olMatrixNode.clique1.cliqueID;
				int id2 = olMatrixNode.clique2.cliqueID;
				int olNum = olMatrixNode.OVNum;// 矩阵对角线上，小于阈值，该值（重叠度）置为0;小于阈值-1，该值（重叠度）置为0;
				if (id1 == id2 && olNum >= myThreshold) {// 矩阵对角线上，大于等于阈值，该值（重叠度）置为1;
					olNum = 1;
				} else if (id1 != id2 && olNum >= myThreshold - 1) {// 矩阵非对角上，大于等于阈值-1，该值（重叠度）置为1
					olNum = 1;
				} else {
					olNum = 0;
				}
				if (olNum > 0) {
					// 合并两个极大团——两个团的id，选择小的id作为parentID
					int pID = (olMatrixNode.clique1.parentID <= olMatrixNode.clique2.parentID)
							? olMatrixNode.clique1.parentID
							: olMatrixNode.clique2.parentID;
					olMatrixNode.clique1.setParentID(pID);
					olMatrixNode.clique2.setParentID(pID);
				}

				return new OLMatrixNode(olMatrixNode.clique1, olMatrixNode.clique2, olNum);
			}
		});

		// 过滤连接矩阵中，值为0的关系
		JavaRDD<OLMatrixNode> myOLConnectSimpleMatrixRDD = myOLConnectMatrixRDD
				.filter(new Function<OLMatrixNode, Boolean>() {

					@Override
					public Boolean call(OLMatrixNode v1) throws Exception {
						// TODO Auto-generated method stub
						boolean flag = false;
						if (v1.OVNum == 1) {
							flag = true;
						}
						return flag;
					}
				});

		myOLConnectSimpleMatrixRDD.foreach(new VoidFunction<OLMatrixNode>() {
			@Override
			public void call(OLMatrixNode t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("id1:" + t.clique1.cliqueID + " id1_parent:" + t.clique1.parentID + " id2:"
						+ t.clique2.cliqueID + " id2_parent:" + t.clique2.parentID);
			}
		});

		// JavaRDD->JavaPairRDD (k,v):k为parentID，v为community
		JavaPairRDD<Integer, Integer> myOLConnectSimpleMatrixPairRDD = myOLConnectSimpleMatrixRDD
				.mapToPair(new PairFunction<OLMatrixNode, Integer, Integer>() {

					@Override
					public Tuple2<Integer, Integer> call(OLMatrixNode olMatrixNode) throws Exception {
						// TODO Auto-generated method stub
						int parentID = (olMatrixNode.clique1.parentID <= olMatrixNode.clique2.parentID)
								? olMatrixNode.clique1.parentID
								: olMatrixNode.clique2.parentID; // K值,parentID,选择两个团的最小的parentID

						int newCliqueID = (olMatrixNode.clique1.cliqueID > olMatrixNode.clique2.cliqueID)
								? olMatrixNode.clique1.cliqueID
								: olMatrixNode.clique2.cliqueID;// V值，存储非parentID的团的id,

						return new Tuple2<Integer, Integer>(parentID, newCliqueID);
					}
				});

		myOLConnectSimpleMatrixPairRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {

			@Override
			public void call(Tuple2<Integer, Integer> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("parentID:" + t._1 + " cliqueID:" + t._2);
			}
		});

		// 将相同parentID的团都分到一组
		JavaPairRDD<Integer, Iterable<Integer>> myOLCommunityPairRDD = myOLConnectSimpleMatrixPairRDD.groupByKey();
		myOLCommunityPairRDD.foreach(new VoidFunction<Tuple2<Integer, Iterable<Integer>>>() {

			@Override
			public void call(Tuple2<Integer, Iterable<Integer>> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("parentID:" + t._1 + " cliqueIDs:" + t._2().toString());
			}
		});

	}

}
