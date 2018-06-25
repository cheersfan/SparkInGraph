package spark.spark0;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @author fxf
 * 
 *         存储图数据的类，实现序列化接口
 * 
 *         使用hashmap存储图数据
 *
 */
public class Graph implements Serializable {

	ArrayList<String> nodes;// 图的节点数据
	HashMap<String, ArrayList<String>> edges; // 图的边的信息

	/**
	 * Graph类的构造函数
	 */
	public Graph() {
		nodes = new ArrayList<>();
		setEdges(new HashMap<>());
	}

	public Graph(Graph g) {
		nodes = new ArrayList<>(g.nodes);
		setEdges(new HashMap<>(g.getEdges()));
	}

	/**
	 * 从路径path中读取图的数据
	 * 
	 * @param path
	 */
	public Graph(String path) {

		nodes = new ArrayList<>();
		setEdges(new HashMap<>());

		File file = new File(path);
		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader(file));
			String str = null;

			while ((str = reader.readLine()) != null) {
				ArrayList<String> line = new ArrayList<>(Arrays.asList(str.split(" ")));
				// System.out.println(line);
				this.addNodes(line.get(0));
				if (line.size() > 1) {
					this.addEdges(line.get(0), new ArrayList<>(line.subList(1, line.size())));
				} else {
					this.addEdges(line.get(0), new ArrayList<>());
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setNodes(ArrayList<String> n) {
		nodes = new ArrayList<>(n);
	}

	public void setEdges(HashMap<String, ArrayList<String>> e) {
		edges = new HashMap<>(e);

	}

	public void addNodes(String n) {
		if (!(nodes.contains(n))) {
			nodes.add(n);
		}
	}

	public void addEdge(String n, String eNode) {
		if (getEdges().containsKey(n)) {
			getEdges().get(n).add(eNode);
		} else {
			ArrayList<String> e = new ArrayList<>();
			e.add(eNode);
			getEdges().put(n, e);
		}
	}

	public void addEdges(String n, ArrayList<String> e) {

		if (getEdges().containsKey(n)) {
			getEdges().get(n).addAll(e);
		} else {
			getEdges().put(n, e);
		}
	}

	public ArrayList<String> getNodes() {
		return nodes;
	}

	public void printGraph() {
		System.out.println("nodes: " + nodes.toString());
		for (String n : nodes) {
			System.out.println("edges: " + n + " -> " + getEdges().get(n).toString());
		}
	}

	/**
	 * 获取第i个节点
	 */
	public String getNode(int t) {
		return nodes.get(t);
	}

	/**
	 * @return the edges
	 */
	public HashMap<String, ArrayList<String>> getEdges() {
		return edges;
	}

	/**
	 * 求节点n的邻点集合
	 */
	public ArrayList<String> getEdges(String n) {
		return edges.get(n);
	}
}