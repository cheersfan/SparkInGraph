package spark.spark0;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author fxf
 *
 *         对Data.txt数据进行处理，生成data4.txt数据文件
 * 
 *         去除属性一栏，并将数据序列化
 * 
 *         在本地运行
 */
public class formatData {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("start");
		File file = new File("D:/data/Data_twitter.txt"); // 读的数据
		File file2 = new File("./data_twitter.txt"); // 写的数据，将读的数据处理后写出来

		BufferedReader reader = null;

		ArrayList<ArrayList<String>> graph = new ArrayList<>();
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file2));
			reader = new BufferedReader(new FileReader(file));
			String str = null;

			while ((str = reader.readLine()) != null) {
				System.out.println(str);
				ArrayList<String> list = new ArrayList<>(Arrays.asList(str.split("\t")));
				if (list.size() >= 2) {
					list.remove(1);
				}
				bw.write(list.get(0) + " ");
				// bw.flush();
				// 为了有序输出
				while (list.size() > 1) {
					int t = Integer.parseInt(list.get(1));
					int t_index = 1;
					for (int k = 1; k < list.size(); k++) {
						if (t > Integer.parseInt(list.get(k))) {
							t = Integer.parseInt(list.get(k));
							t_index = k;
						}
					}
					bw.write(list.get(t_index) + " ");
					bw.flush();
					list.remove(t_index);
				}

				bw.newLine();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("final");
	}

}
