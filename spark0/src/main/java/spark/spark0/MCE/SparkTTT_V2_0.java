package spark.spark0.MCE;

import java.util.ArrayList;

import org.apache.commons.collections.ListUtils;

import spark.spark0.Graph;

/**
 * @author fxf
 *
 */
public class SparkTTT_V2_0 {
	ArrayList<ArrayList<String>> cliques = new ArrayList<>();// 最终求得的全部极大团数据
	Graph graph;// 图数据

	/**
	 * @param list
	 * @param graph_bc_value
	 * @return
	 */
	public ArrayList<ArrayList<String>> TTTMethod(ArrayList<String> list, Graph graph_bc_value) {
		// TODO Auto-generated method stub
		graph = new Graph(graph_bc_value);// 初始化grah对象

		ArrayList<String> SUBG = new ArrayList<>();
		ArrayList<String> CAND = new ArrayList<>();
		ArrayList<String> Q = new ArrayList<>();

		String startNode = list.get(0);
		for (String node : list) {
			if (Integer.parseInt(node) >= Integer.parseInt(startNode)) {
				CAND.add(node);
			}
		}
		SUBG.addAll(list);

		// 递归调用TTTAlgorithm方法
		TTTAlgorithm(SUBG, CAND, Q);

		return cliques;
	}

	/**
	 * @param SUBG
	 * @param CAND
	 * @param Q
	 * @return
	 */
	public void TTTAlgorithm(ArrayList<String> SUBG, ArrayList<String> CAND, ArrayList<String> Q) {
		// TODO Auto-generated method stub
		// System.out.println("SUBG: " + SUBG.toString() + " CAND: " + CAND.toString() +
		// " Q: " + Q.toString());
		if (SUBG.isEmpty()) {// 求出极大团的条件
			// System.out.println("******cliques:" + Q.toString() + "******");
			cliques.add(new ArrayList<>(Q));
			// System.out.println();
		} else {
			if (!(SUBG.isEmpty() || CAND.isEmpty())) {
				// 步骤1：为了尽可能地剪枝，找到点u，使得：CAND-neibor(u)的集合大小最小
				String uNode = findNodeU(SUBG, CAND);
				// System.out.println("uNode: " + uNode);

				// 步骤2：计算待扩展集合CAND-neibor(u)
				ArrayList<String> expendNodes = new ArrayList<>(ListUtils.subtract(CAND, graph.getEdges(uNode)));
				// System.out.println("expendNodes: " + expendNodes.toString());

				// 步骤3：遍历待扩展集合中的每一个点，求极大团
				for (String node : expendNodes) {
					// System.out.println("expandNode:" + node);
					// System.out.println(
					// "expand SUBG: " + SUBG.toString() + " CAND: " + CAND.toString() + " Q: " +
					// Q.toString());
					Q.add(node);
					ArrayList<String> nSUBG = new ArrayList<>(ListUtils.intersection(SUBG, graph.getEdges(node)));
					ArrayList<String> nCAND = new ArrayList<>(ListUtils.intersection(CAND, graph.getEdges(node)));

					TTTAlgorithm(nSUBG, nCAND, Q);
					// System.out.println("removeExpandNode: " + node);
					CAND.remove(node);
					Q.remove(node);
				}
			}
		}
	}

	/**
	 * 寻找SUBG里面的点u，使得CAND-neibor(u)的集合大小最小
	 * 
	 * @param SUBG
	 * @param CAND
	 * @return
	 */
	private String findNodeU(ArrayList<String> SUBG, ArrayList<String> CAND) {
		// TODO Auto-generated method stub
		// System.out.println("SUBG: " + SUBG.toString() + " CAND: " + CAND.toString());

		int minCount = CAND.size() - 1;// CAND-neibor(u)集合的大小
		String minNode = SUBG.get(0);// 当前最小集合的节点的index

		for (String node : SUBG) {
			int count = ListUtils.subtract(CAND, graph.getEdges(node)).size();
			// System.out.println(ListUtils.subtract(CAND,
			// graph.getEdges(node)).toString());
			// System.out.println(ListUtils.subtract(CAND, graph.getEdges(node)).size());
			if (count < minCount) {
				minCount = count;
				minNode = node;
			}
		}
		// System.out.println("minCount: " + minCount + " minNode: " + minNode);

		return minNode;
	}

}
