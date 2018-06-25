package spark.spark0.kcommunity;

import java.util.ArrayList;

import spark.spark0.Graph;

/**
 * @author fxf
 *
 *         求得一个集合里（这个集合里有多个连通分支）的全部的连通分支
 */
public class MergeSet {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		long startMergeTime = System.currentTimeMillis();// 开始时间

		String graphPath = "CA-CondMat_23K_format.txt";
		String cliquesPath = "CA-CondMat_23K_cliques.txt";
		String outputPath = "CA-CondMat_23K_comm.txt";
		// String graphPath = "data_50.txt";
		// String cliquesPath = "data_50_cliques.txt";
		// String outputPath = "data_50_comm.txt";

		// 从文件file中读取graph数据，创建graph对象
		Graph myGraph = new Graph(graphPath);
		// 从文件file中读取cliques数据，创建cliques对象，已按照团的节点个数排序
		Cliques myCliques = new Cliques(cliquesPath);

		long readCliqueTime = System.currentTimeMillis();// 读文件时间

		// myGraph.printGraph();
		// myCliques.printCliques();

		// 遍历graph的每一条边
		for (int i = 0; i < myGraph.getNodes().size(); i++) {
			String pNode = myGraph.getNode(i);
			ArrayList<String> pEdges = new ArrayList<>(myGraph.getEdges(pNode));// 包含点p的全部边集合
			for (String qNode : pEdges) {
				// pNode->p, qNode->q
				int pID = myCliques.findF(pNode); // 在全部的集合中，查找一个包含pNode的集合id，pID
				int qID = myCliques.findF(qNode);
				myCliques.unionF(pID, qID);
			}
		}

		myCliques.outputCommunities(outputPath);
		long endMergeTime = System.currentTimeMillis();
		System.out.println("读取文件时间：" + (readCliqueTime - startMergeTime) / 1000 + " s");
		System.out.println("Merge时间： " + (endMergeTime - readCliqueTime) / 1000 + " s");

		// for (Entry<Integer, ArrayList<String>> entry :
		// myCliques.getCliques().entrySet()) {
		// System.out.println("index: " + entry.getKey() + " community: " +
		// entry.getValue().toString()
		// + " nodeCount: " + entry.getValue().size());
		// }
	}
}
