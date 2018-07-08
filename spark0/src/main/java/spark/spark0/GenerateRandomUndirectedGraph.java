package spark.spark0;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 * @author fxf
 * 
 *         生成给定节点个数的无向图
 *
 */
public class GenerateRandomUndirectedGraph {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int nodeCount = 500;// 节点的总数
		String dataOutPutPath = "E:/JavaProject/graph_data/graph_" + nodeCount + "_format.txt";

		Graph graph = new Graph();

		ArrayList<Integer> nodeEdgeCount = new ArrayList<>();
		ArrayList<ArrayList<String>> nodesEdges = new ArrayList();// node1->node2
		HashMap<String, ArrayList<String>> edgesHashMap = new HashMap<>();// node->edgeNodes
		Random random = new Random();

		// node点->t个邻边
		int edgeCound = 0;
		for (int i = 0; i < nodeCount; i++) {
			int t = random.nextInt(50);
			nodeEdgeCount.add(t);
			edgeCound += t;
			System.out.println(i + " " + t);
		}
		System.out.println("edgeCount:" + edgeCound);

		// 生成node点的t个邻边，不能重复
		for (int i = 0; i < nodeCount; i++) {
			int node = i;// 节点node
			// 生成node点的t个不相同的邻边
			ArrayList<String> edges = new ArrayList<>();
			for (int j = 0; j < nodeEdgeCount.get(i); j++) {
				int node2 = random.nextInt(nodeCount);
				// node2不能和node相同，不能和之前的node之前的邻点相同
				while (node == node2 || edges.contains(node2 + "")) {
					node2 = random.nextInt(nodeCount);
				}
				edges.add(node2 + "");
			}
			System.out.println("node:" + node + " edges:" + edges.toString());

			for (int j = 0; j < edges.size(); j++) {
				ArrayList<String> list1 = new ArrayList<>();
				ArrayList<String> list2 = new ArrayList<>();

				list1.add(node + "");
				list1.add(edges.get(j));
				nodesEdges.add(list1);

				list2.add(edges.get(j));
				list2.add(node + "");
				nodesEdges.add(list2);
			}
		}

		// 初始化图对象
		ArrayList<String> nodesList = new ArrayList<>();
		for (int i = 0; i < nodeCount; i++) {
			nodesList.add(i + "");
		}

		graph.setNodes(nodesList);

		for (int i = 0; i < nodesEdges.size(); i++) {
			System.out.println("addEdges:" + nodesEdges.get(i).get(0) + " " + nodesEdges.get(i).get(1));
			graph.addEdge(nodesEdges.get(i).get(0), nodesEdges.get(i).get(1));
		}

		graph.printGraph();
		System.out.println(graph.getEdgeCount());

		BufferedReader reader = null;
		File file = new File(dataOutPutPath);

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			for (String n : graph.nodes) {
				bw.write(n + " ");
				for (int i = 0; i < graph.getEdges().get(n).size(); i++) {
					bw.write(graph.getEdges().get(n).get(i) + " ");
				}
				bw.newLine();
				bw.flush();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
