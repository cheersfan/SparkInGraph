package spark.spark0;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

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
		String readme = "D:\\spark\\CHANGES.txt";
		SparkConf conf = new SparkConf().setAppName("tiger's first spark app").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(readme).cache();
		long num = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("a");
			}

		}).count();

		JavaRDD<String> logData2 = logData.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return v1.contains("a");
			}
		});

		long num2 = logData2.count();

		System.out.println("the count of word a is " + num);

	}

}
