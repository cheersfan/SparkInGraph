package spark.spark0.kcommunity;

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
		/* 测试合并两个类型相同的list */
		/*
		 * List<String> list1 = new ArrayList<String>(); List<String> list2 = new
		 * ArrayList<String>(); // 给list1赋值 list1.add("合"); list1.add("试");
		 * list1.add("一"); list1.add("下"); // 给list2赋值 list2.add("合"); list2.add("并");
		 * list2.add("列"); list2.add("表"); // 将list1.list2合并 list1.addAll(list2); //
		 * 循环输出list1 看看结果 for (String s : list1) { System.out.print(s); }
		 */

		ArrayList<String> clique1 = new ArrayList<>();
		clique1.add("1");
		clique1.add("2");
		clique1.add("3");
		clique1.add("4");
		clique1.add("5");
		clique1.add("6");

		ArrayList<String> clique2 = new ArrayList<>();
		clique2.add("1");
		clique2.add("2");
		clique2.add("3");
		clique2.add("4");
		clique2.add("5");
		clique2.add("6");

		int count = 0;
		for (int i = 0; i < clique1.size(); i++) {
			count = clique2.contains(clique1.get(i)) ? count++ : count;
		}
		System.out.println(count);
	}

}
