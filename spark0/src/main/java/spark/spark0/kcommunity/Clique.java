package spark.spark0.kcommunity;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author fxf
 *
 */
public class Clique implements Serializable {
	int cliqueID;
	int parentID;
	ArrayList<String> cliqueInfo;

	/**
	 * 构造函数
	 * 
	 * @param id
	 *            clique的id
	 * @param clique
	 *            clique的具体节点 构造函数
	 */
	public Clique(int id, ArrayList<String> clique) {
		// TODO Auto-generated constructor stub
		this.cliqueID = id;
		this.parentID = id;
		this.cliqueInfo = new ArrayList<>(clique);
	}

	public Clique(int id, int pid, ArrayList<String> clique) {
		this.cliqueID = id;
		this.parentID = pid;
		this.cliqueInfo = new ArrayList<>(clique);
	}

	public void setParentID(int pID) {
		this.parentID = pID;
	}

}
