package spark.spark0;

import java.util.ArrayList;
import java.util.Random;

/**
 * @author fxf
 * 
 *         方法一：生成给定的点数的随机图 方法二：生成随机无向图
 *
 */
public class RandomGraph {
	static Graph graph = new Graph();

	/**
	 * 
	 */
	public RandomGraph() {
		// TODO Auto-generated constructor stub
	}

	public static Graph generateGraph(int n) {

		Random random = new Random(500);
		for (int i = 0; i < n; i++) {
			graph.nodes.add("" + i);
			ArrayList<String> edges = new ArrayList<>();

			int edge_num = Math.abs(random.nextInt()) % n;
			for (int j = 0; j < edge_num; j++) {// 生成n个点的图
				String t = Math.abs(random.nextInt()) % n + "";
				if (!edges.contains(t) && !t.equals(i + "") && i < Integer.parseInt(t)) {
					edges.add(t);
				}

			}
			graph.addEdges(i + "", edges);
		}
		return graph;
	}

	public Graph generateUndirectedGraph(int n) {
		Random random = new Random(100);
		// 随机生成节点
		int nodeCount = Math.abs(random.nextInt()) % n; // 节点的个数
		System.out.println("nodeCount:" + nodeCount);
		for (int i = 0; i < nodeCount; i++) {
			String node = Math.abs(random.nextInt()) % n + "";
			if (!graph.nodes.contains(node)) {
				graph.addNodes(node);
				graph.edges.put(node, new ArrayList<>());
			} else {
				nodeCount--;
			}
		}
		nodeCount--;

		System.out.println(graph.nodes.toString() + " count:" + graph.nodes.size());
		for (String node : graph.nodes) {
			int edgeCount = Math.abs(random.nextInt()) % nodeCount; // 生成边的个数
			while (edgeCount > 0) {
				int nEdgeIndex = Math.abs(random.nextInt()) % nodeCount;
				String nEdge = graph.nodes.get(nEdgeIndex); // 生成另一个端点
				if (!nEdge.equals(node) && !graph.edges.get(node).contains(nEdge)) {
					graph.addEdge(node, nEdge); // 生成一个顶点，两个端点添加两次
					graph.addEdge(nEdge, node);
				}
				edgeCount--;
			}
		}

		return graph;
	}

	public static ArrayList<ArrayList<String>> generateAdjGraph() {
		ArrayList<ArrayList<String>> adjGraph = new ArrayList<>();
		for (int i = 0; i < graph.nodes.size(); i++) {
			ArrayList<String> edges = new ArrayList<>();
			String n = graph.nodes.get(i);
			edges.add(n);
			edges.addAll(graph.getEdges().get(n));
			adjGraph.add(edges);
		}
		return adjGraph;
	}

}
