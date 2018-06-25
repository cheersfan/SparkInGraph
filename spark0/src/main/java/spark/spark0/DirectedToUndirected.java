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
 *         处理图数据：有向图变为无向图，数据量会减少
 */
public class DirectedToUndirected {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("start");
		File file = new File("./data_youtube.txt"); // 读的数据
		File file2 = new File("./data_youtube_undirected.txt"); // 写的数据，将读的数据处理后写出来

		BufferedReader reader = null;
		ArrayList<ArrayList<String>> graph = new ArrayList<>();
		try {
			reader = new BufferedReader(new FileReader(file));
			String str = null;

			while ((str = reader.readLine()) != null) {
				ArrayList<String> strs = new ArrayList<>(Arrays.asList(str.split(" ")));
				graph.add(strs);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file2));
			for (int i = 0; i < graph.size(); i++) {
				bw.write(graph.get(i).get(0) + " ");
				bw.flush();

				int j = 1;
				// System.out.println(graph.get(i).size());
				while (j < graph.get(i).size()) {
					if (Integer.parseInt(graph.get(i).get(j)) > Integer.parseInt(graph.get(i).get(0))) {
						System.out.println(graph.get(i).get(j) + " " + graph.get(i).get(0));
						bw.write(graph.get(i).get(j) + " ");
						bw.flush();
					}
					j++;
				}

				bw.newLine();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("final++++");
	}

}
