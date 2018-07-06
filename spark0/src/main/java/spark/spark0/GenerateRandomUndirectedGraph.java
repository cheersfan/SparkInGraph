package spark.spark0;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
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
		int nodeCount = 50;// 节点的总数
		String dataOutPutPath = "E:/JavaProject/graph_data/graph_" + nodeCount + "_origin.txt";

		HashMap<String, Integer> nodesEdgesCount = new HashMap<>();// node->count 节点对应其边的个数
		ArrayList<ArrayList<String>> nodesEdges = new ArrayList();// node1->node2
		Random random = new Random();

		// node点->t个邻边
		for (int i = 0; i < nodeCount; i++) {
			int t = random.nextInt(nodeCount);
			nodesEdgesCount.put(i + "", t);
		}

		// 生成node点的t个邻边
		for (Entry<String, Integer> node : nodesEdgesCount.entrySet()) {
			for (int i = 0; i < node.getValue(); i++) {
				ArrayList<String> list = new ArrayList<>();
				int node2 = random.nextInt(nodeCount);
				if (node.getKey().equals(node2 + "")) {
					node2 = node2 + random.nextInt(nodeCount - node2);
				}
				list.add(node.getKey());
				list.add(node2 + "");
				nodesEdges.add(list);
			}
		}

		// 将生成的图输出到文件里
		File file = new File(dataOutPutPath);
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			for (ArrayList<String> list : nodesEdges) {
				bw.write(list.get(0) + " " + list.get(1));
				bw.flush();
				bw.newLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
