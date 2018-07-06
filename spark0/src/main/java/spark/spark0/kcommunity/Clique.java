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

	public Clique(Clique c) {
		this.cliqueID = c.cliqueID;
		this.cliqueInfo = new ArrayList<>(c.cliqueInfo);
		this.parentID = c.parentID;
	}

	public Clique(int id, int pid, ArrayList<String> clique) {
		this.cliqueID = id;
		this.parentID = pid;
		this.cliqueInfo = new ArrayList<>(clique);
	}

	/**
	 * 重写equals方法
	 * 
	 * @param pID
	 */
	public boolean equals(Object obj) {
		if (obj instanceof Clique) {
			Clique cObj = (Clique) obj;
			return this.cliqueID == cObj.cliqueID && this.parentID == cObj.parentID
					&& this.cliqueInfo.equals(cObj.cliqueInfo);
		}
		return super.equals(obj);// ???

	}

	/**
	 * parentID的Set方法
	 * 
	 * @param pID
	 */
	public void setParentID(int pID) {
		this.parentID = pID;
	}

	public void printClique() {
		System.out.print("cid:" + cliqueID);
	}

}
