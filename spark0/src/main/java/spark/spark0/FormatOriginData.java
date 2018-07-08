package spark.spark0;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author fxf
 *
 */
public class FormatOriginData {

	private static String n;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("start");
		// File file = new File("D:/data/as-caida20071105.txt/as-caida20071105.txt"); //
		// 读的数据
		// File file2 = new
		// File("E:/JavaProject/graph_data/as-caida20071105_format.txt"); //
		// 写的数据，将读的数据处理后写出来

		File file = new File("E:/JavaProject/graph_data/graph_20_origin.txt"); // 读的数据
		File file2 = new File("E:/JavaProject/graph_data/graph_20_format.txt"); // 写的数据，将读的数据处理后写出来

		BufferedReader reader = null;
		Graph graph = new Graph();

		try {
			reader = new BufferedReader(new FileReader(file));
			String str = null;

			while ((str = reader.readLine()) != null) {
				System.out.println(str);
				ArrayList<String> list = new ArrayList<>(Arrays.asList(str.split(" ")));
				graph.addNodes(list.get(0));
				graph.addEdge(list.get(0), list.get(1));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file2));
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
