package spark.spark0;

/**
 * @author fxf
 *
 */
public class main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// // 从file中读取，创建broadcast变量
		// File file = new File("data4.txt");
		// BufferedReader reader = null;
		// Graph graph = new Graph();
		// ArrayList<ArrayList<String>> adjGraph = new ArrayList<>();
		// try {
		// reader = new BufferedReader(new FileReader(file));
		// String str = null;
		//
		// while ((str = reader.readLine()) != null) {
		// ArrayList<String> line = new ArrayList<>(Arrays.asList(str.split(" ")));
		// // System.out.println(line);
		// adjGraph.add(line);
		// graph.addNodes(line.get(0));
		// if (line.size() > 1) {
		// graph.addEdges(line.get(0), new ArrayList<>(line.subList(1, line.size())));
		// } else {
		// graph.addEdges(line.get(0), new ArrayList<>());
		// }
		// }
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// graph.getEdges("5");

		// HashMap<Integer, String> hMap = new HashMap<>();
		// hMap.put(1, "one");
		// hMap.put(2, "two");
		// hMap.put(3, "three");
		// hMap.remove(3);
		// for (Entry<Integer, String> entry : hMap.entrySet()) {
		// System.out.println(entry.getKey() + " " + entry.getValue());
		// }

		RandomGraph randowGraph = new RandomGraph();
		Graph graph = randowGraph.generateUndirectedGraph(50);
		graph.printGraph();
	}

}
