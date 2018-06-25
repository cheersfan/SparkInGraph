package spark.spark0.kcommunity;

import java.io.Serializable;

/**
 * @author fxf
 *
 */
public class OLMatrixNode implements Serializable {
	Clique clique1;
	Clique clique2;
	int OVNum;

	public OLMatrixNode(Clique c1, Clique c2, int num) {
		this.clique1 = c1;
		this.clique2 = c2;
		this.OVNum = num;
	}

	public void setOVNum(int num) {
		this.OVNum = num;
	}

}
