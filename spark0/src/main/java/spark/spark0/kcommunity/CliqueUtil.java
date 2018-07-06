package spark.spark0.kcommunity;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * 
 * Clique的工具类 读取clique文件、写求得的社区数据文件等
 * 
 * @author fxf
 *
 */
public class CliqueUtil implements Serializable {
	public CliqueUtil() {

	}

	/**
	 * 从文件路径中读取图数据
	 * 
	 * @param path
	 * @return 图数据的数组
	 */
	public ArrayList<Clique> getCliquesFromPath(String path) {
		ArrayList<Clique> cliques = new ArrayList<>();
		File file = new File(path);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = null;
			int id = 1;
			while ((line = reader.readLine()) != null) {
				// 处理读取的一行数据
				line = line.replace("[", "");
				line = line.replace("]", "");
				if (line.length() > 0) {
					Clique clique = new Clique(id++, new ArrayList<>(Arrays.asList(line.split(", "))));// 每一行，即为一个Clique对象
					cliques.add(clique);
				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return cliques;
	}

	/**
	 * 将求得的社区数据写入文件中
	 * 
	 * @param path
	 */
	public void writeCommToPath(String path, HashMap<Integer, ArrayList<Clique>> comms) {
		File file = new File(path);
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			for (Entry<Integer, ArrayList<Clique>> entry : comms.entrySet()) {
				bw.write("pid: " + entry.getKey() + " cids: ");
				for (Clique clique : entry.getValue()) {
					bw.write(clique.cliqueInfo.toString() + " ");
				}
				bw.flush();
				bw.newLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param clique1
	 *            第一个团的对象
	 * @param clique2
	 *            第二个团的对象
	 * @return 两个团的重叠的个数
	 */
	public int getOLNum(Clique clique1, Clique clique2) {
		int OLNum = 0;

		for (String c1_node : clique1.cliqueInfo) {
			for (String c2_node : clique2.cliqueInfo) {
				if (c1_node.equals(c2_node)) {
					OLNum++;
				}
			}
		}

		return OLNum;
	}

}
