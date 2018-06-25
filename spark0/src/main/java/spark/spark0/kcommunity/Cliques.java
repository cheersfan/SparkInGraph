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
 * @author fxf 读取的cliqes数据的对象
 *         实现Serializable接口，使本类可串行化，从而可以在spark中使用广播变量发送Cliques类的对象
 */
public class Cliques implements Serializable {

	private String filePath; // 存储团数据的文件路径
	private HashMap<Integer, ArrayList<String>> cliques; // HashMap的属性：<cliqueID,cliqueInfo>
	private HashMap<Integer, Integer> nCountClique;
	private Integer cliquesCount;

	/**
	 * @param path
	 *            存储团数据的文件路径
	 */
	public Cliques(String path) {
		// TODO Auto-generated constructor stub
		filePath = path;
		cliques = new HashMap<>();
		nCountClique = new HashMap();

		readCliquesFromFile(filePath); // 从文件中读取cliques数据
	}

	/**
	 * 从文件中读取cliques数据
	 * 
	 * @param path
	 *            文件路径
	 * 
	 */
	private void readCliquesFromFile(String path) {

		File file = new File(path);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));

			String str = null; // 存取当前读入的一行数据
			Integer index = 0; // 当前团的id
			while ((str = reader.readLine()) != null) {
				// 处理读取的行数据
				str = str.replace("[", "");
				str = str.replace("]", "");
				cliques.put(index, new ArrayList<>(Arrays.asList(str.split(", "))));
				nCountClique.put(index, cliques.get(index).size());
				index++;
			}
			cliquesCount = index - 1;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 获取cliques数据，
	 * 
	 * 以数组的形式返回数据，内容为：index + clique
	 * 
	 * @return
	 */
	public ArrayList<ArrayList<String>> getCliquesList() {
		ArrayList<ArrayList<String>> cliquesList = new ArrayList<>();

		// 遍历cliques
		for (Entry<Integer, ArrayList<String>> entry : cliques.entrySet()) {
			ArrayList<String> clique = new ArrayList<>();
			clique.add(entry.getKey() + "");
			clique.addAll(entry.getValue());
			cliquesList.add(clique);
		}

		return cliquesList;
	}

	/**
	 * 在全部的团里寻找节点node所在的团的id
	 * 
	 * @param node
	 *            所要查询的节点
	 * @return idNodeClique 节点node所在团的id
	 */
	public int findF(String node) {

		int idNodeCliques = 0;
		for (Entry<Integer, ArrayList<String>> entry : cliques.entrySet()) {
			if (entry.getValue().contains(node)) {
				idNodeCliques = entry.getKey();
				break; // 找到一个就停止
			}
		}
		return idNodeCliques;
	}

	/**
	 * 在全部的团里寻找节点node所在的团的id
	 * 
	 * @param node
	 *            所要查询的节点
	 * @return idNodeClique 节点node所在团的id
	 */
	public ArrayList<Integer> findFs(String node) {

		ArrayList<Integer> nIDs = new ArrayList<>();
		for (Entry<Integer, ArrayList<String>> entry : cliques.entrySet()) {
			if (entry.getValue().contains(node)) {
				nIDs.add(entry.getKey());// 找到全部就停止
			}
		}
		return nIDs;
	}

	/**
	 * 将两个id为pID和qID的团合并，新生成的集合id为pID
	 * 
	 * @param pID
	 * @param qID
	 */
	public void unionF(int pID, int qID) {
		// 将qID的团合并到pID的团中
		if (pID < qID) {
			// System.out.println("pID:" + pID + " qID:" + qID);
			// System.out.println(cliques.get(0));
			ArrayList<String> pList = new ArrayList<>(cliques.get(pID));
			ArrayList<String> qList = new ArrayList<>(cliques.get(qID));
			for (String n : qList) {
				if (!(pList.contains(n))) {
					pList.add(n);
				}
			}
			cliques.replace(pID, pList);
			cliques.remove(qID);
			// if (cliques.containsKey(qID)) {
			// System.out.println("have qID:" + cliques.get(qID));
			// }
		}

	}

	/**
	 * 获取cliques全部数据
	 * 
	 * @return
	 */
	public HashMap<Integer, ArrayList<String>> getCliques() {
		return this.cliques;
	}

	/**
	 * 获取cliques中index为n的团
	 * 
	 * @param n
	 * @return
	 */
	public ArrayList<String> getClique(int n) {
		return cliques.get(n);
	}

	/**
	 * 输出整个图
	 */
	public void printCliques() {
		for (Entry<Integer, ArrayList<String>> entry : cliques.entrySet()) {
			System.out.println("index: " + entry.getKey() + " clique: " + entry.getValue().toString());
		}
	}

	/**
	 * 将计算出的连通分支输出到文件
	 */
	public void outputCommunities(String path) {
		File file = new File(path);
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			for (Entry<Integer, ArrayList<String>> entry : cliques.entrySet()) {
				bw.write(entry.getValue().toString());
				bw.flush();
				bw.newLine();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
