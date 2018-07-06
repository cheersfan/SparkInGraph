package local.MCE;

import java.util.ArrayList;

/**
 * @author fxf
 *
 */
public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// BK bk = new BK();
		// bk.BK("E:/JavaProject/graph_data/graph_9_format.txt");
		// System.out.println("CLIQUES");
		// for (ArrayList<String> clique : bk.cliques) {
		// System.out.println("Clique: " + clique.toString());
		// }

		TTT ttt = new TTT();
		ttt.TTTMethod("E:/JavaProject/graph_data/graph_9_format.txt");
		System.out.println("CLIQUES");
		for (ArrayList<String> clique : ttt.cliques) {
			System.out.println("Clique: " + clique.toString());
		}

	}

}
