package local.MCE;

import java.util.ArrayList;

import spark.spark0.Graph;

/**
 * @author fxf
 *
 */
public class BK {
	ArrayList<ArrayList<String>> cliques = new ArrayList<>();
	Graph graph;// 图数据

	/**
	 * @param args
	 */
	public void BK(String path) {
		// TODO Auto-generated method stub
		graph = new Graph(path);// 从路径中读取图数据

		graph.printGraph();

		for (String node : graph.getNodes()) {// 遍历graph全部的点，以及此点的边
			// BK算法的三个集合
			ArrayList<String> COMSUB = new ArrayList<>();
			ArrayList<String> CAND = new ArrayList<>();
			ArrayList<String> NOT = new ArrayList<>();

			CAND.add(node);
			// 避免重复计算：eg. 1 2 9, 2 1 9, 9 1 2
			for (String edge : graph.getEdges(node)) {
				if (Integer.parseInt(edge) < Integer.parseInt(node)) {
					NOT.add(edge);
				} else {
					CAND.add(edge);
				}
			}

			// 递归调用BK方法
			BKAlgorithm(COMSUB, CAND, NOT);
		}
	}

	public void BKAlgorithm(ArrayList<String> comsub, ArrayList<String> cand, ArrayList<String> not) {
		// System.out.println();

		if (not.isEmpty() && cand.isEmpty()) {// 计算出团的条件
			cliques.add(comsub);
		} else if (!cand.isEmpty()) {
			for (int i = 0; i < cand.size(); i++) {
				String p = cand.get(0);
				// 步骤1：选择扩展定点p
				ArrayList<String> pNeibors = new ArrayList<>(graph.getEdges(p));// p的邻点

				// 步骤2：将点p加入到COMSUB集合中
				ArrayList<String> nCOMSUB = new ArrayList<>(getUnion(comsub, p));

				// 步骤3：创建nCAND，nNOT
				ArrayList<String> nCAND = new ArrayList<>(getIntersection(pNeibors, cand));
				ArrayList<String> nNOT = new ArrayList<>(getIntersection(pNeibors, not));

				// 步骤4：递归调用方法
				BKAlgorithm(nCOMSUB, nCAND, nNOT);

				// 步骤5：从comsub中移除扩展点p，加入到not集合中
				not.add(p);
				comsub.remove(p);
				cand.remove(p);
			}
		}
	}

	/**
	 * 将p点加入到list中
	 * 
	 * @param list
	 * @param p
	 * @return
	 */
	public ArrayList<String> getUnion(ArrayList<String> list, String p) {
		// TODO Auto-generated method stub
		list.add(p);
		return list;
	}

	/**
	 * 两个集合求交
	 * 
	 * @param pNeibors
	 * @param cand
	 * @return
	 */
	public ArrayList<String> getIntersection(ArrayList<String> listA, ArrayList<String> listB) {
		// TODO Auto-generated method stub

		ArrayList<String> listAns = new ArrayList<>();
		for (String node : listA) {
			if (listB.contains(node)) {
				listAns.add(node);
			}
		}
		return listAns;
	}

}
