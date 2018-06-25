package spark.spark0.MCE;

import java.util.ArrayList;

import spark.spark0.Graph;

/**
 * @author fxf
 * 
 *         极大团挖掘的TTT算法，由SparkMCEx调用，计算极大团 使用graph类存储图数据
 */
public class SparkTTT2 {
	public Graph graph; // 图的全部信息
	public ArrayList<String> nodes; // 所求极大团的节点的集合
	public ArrayList<ArrayList<String>> cliques;// 求得的全部团

	public SparkTTT2(Graph g, ArrayList<String> n) {
		// TODO Auto-generated constructor stub
		cliques = new ArrayList<>();
		graph = new Graph(g);
		nodes = new ArrayList<>(n);
	}

	public ArrayList<ArrayList<String>> TTTMCE() {
		int i = 0;

		// System.out.println("nodes" + nodes.toString());
		ArrayList<String> SUBG = new ArrayList<>(nodes);
		ArrayList<String> CAND = new ArrayList<>(nodes);
		ArrayList<String> Q = new ArrayList<>();
		TTTAlgth(SUBG, CAND, Q); // 调用递归函数

		return cliques;

	}

	/**
	 * @param SUBG
	 *            存放与当前集合Q中所有顶点均相邻的点，
	 * @param CAND
	 *            存放从下一步带扩充顶点的集合
	 * @param Q
	 *            存放求得极大团的顶点
	 */
	private void TTTAlgth(ArrayList<String> SUBG, ArrayList<String> CAND, ArrayList<String> Q) {
		// TODO Auto-generated method stub
		// System.out.println("SUBG:" + SUBG.toString());
		// System.out.println("CAND:" + CAND.toString());
		// System.out.println("Q:" + Q.toString());
		if (SUBG.isEmpty()) {// 求出极大团的条件，SUBG为空
			if (!(Q.isEmpty())) {
				ArrayList<String> clique = new ArrayList<>(Q);
				cliques.add(clique);
				// System.out.println("clique:" + clique.toString());
			}

		} else {

			String v = SUBG.get(0);// SUBG中的点v，其邻点与CAND集合相交所得集合中节点数最多
			ArrayList<String> vNeibors = new ArrayList<>(graph.getEdges(v)); // 获得顶点v的全部邻点
			// System.out.println("vNeibors:" + v + " -> " + vNeibors.toString());
			for (int i = 0; i < CAND.size(); i++) {
				if (Integer.parseInt(v) > Integer.parseInt(CAND.get(i))) {// 通过顶点偏序策略来解决计算冗余问题
					CAND.remove(i);
				}
			}

			ArrayList<String> qs = getSubtraction(CAND, vNeibors);
			for (String q : qs) {
				// System.out.println("q:" + q);
				ArrayList<String> qNeibors = new ArrayList<>(graph.getEdges(q));
				// System.out.println("q:" + q + " -> neibors:" + qNeibors.toString());

				Q.add(q);

				ArrayList<String> SUBGnew = getIntersection(SUBG, qNeibors);// 新的SUBG集合
				ArrayList<String> CANDnew = getIntersection(CAND, qNeibors);// 新的CAND集合
				TTTAlgth(SUBGnew, CANDnew, Q);// 递归调用
				Q.remove(q);// 递归调用结束后，将q从集合Q中移除
				CAND.remove(q);// 同上
			}
		}
	}

	/**
	 * @param cAND
	 * @param vNeibors
	 * @return
	 * 
	 * 		集合相减
	 */
	private ArrayList<String> getSubtraction(ArrayList<String> cAND, ArrayList<String> vNeibors) {
		// TODO Auto-generated method stub
		ArrayList<String> ans = new ArrayList<>(cAND);
		// System.out.println("ans:" + ans.toString());
		// System.out.println("vNebors:" + vNeibors.toString());
		ans.removeAll(vNeibors);
		// System.out.println("final_ans:" + ans.toString());
		return ans;
	}

	/**
	 * @param cAND
	 * @param q
	 * @return
	 * 
	 * 		集合相减
	 */
	private ArrayList<String> getSubtraction(ArrayList<String> cAND, String q) {
		// TODO Auto-generated method stub
		ArrayList<String> ans = new ArrayList<>(cAND);
		if (ans.contains(q)) {
			ans.remove(q);
		}
		return ans;
	}

	/**
	 * @param sUBG
	 * @param qNeibors
	 * @return
	 * 
	 * 		求交集
	 */
	private ArrayList<String> getIntersection(ArrayList<String> sUBG, ArrayList<String> qNeibors) {
		// TODO Auto-generated method stub
		ArrayList<String> ans = new ArrayList<>();
		for (String n : sUBG) {
			if (qNeibors.contains(n)) {
				ans.add(n);
			}
		}
		return ans;
	}

	/**
	 * @param sUBG
	 * @param cAND
	 * @return
	 * 
	 * 		在sUBG集合中寻找点v，其邻点与CAND集合相交所得的集合中节点个数最多
	 */
	private String findVS(ArrayList<String> sUBG, ArrayList<String> cAND) {
		// TODO Auto-generated method stub
		int maxCount = -1;
		String maxNode = "";
		for (String n : sUBG) {
			// 统计当前sUBG中的点的邻点集合和cAND集合相交集合内点的个数
			ArrayList<String> vNeibors = new ArrayList<>(graph.getEdges(n));
			ArrayList<String> intersectionAns = getIntersection(vNeibors, cAND);
			if (maxCount < intersectionAns.size()) {
				maxCount = intersectionAns.size();
				maxNode = n;
			}
		}
		return maxNode;
	}
}
