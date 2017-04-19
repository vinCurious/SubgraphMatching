
/* 
 * SubgraphTest.java 
 * 
 * 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

/**
 * This SubgraphTest class implements naive and graphQL subgraph matching and
 * compares the results.
 * 
 * @author Vinay Vasant More
 *
 */
public class SubgraphTest {

	static HashMap<Integer, String> verticesSet = new HashMap<Integer, String>();
	static HashMap<Integer, ArrayList<Integer>> relationshipSet = new HashMap<Integer, ArrayList<Integer>>();
	static ArrayList<ArrayList<Node>> solutionSet = new ArrayList<ArrayList<Node>>();
	static HashMap<Integer, ArrayList<Node>> searchSpace = new HashMap<Integer, ArrayList<Node>>();
	static ArrayList<Node> tempSolution = new ArrayList<Node>();
	static HashMap<Integer, Integer> finalOrder = new HashMap<Integer, Integer>();
	static GraphDatabaseService db;

	/**
	 * The main program.
	 *
	 * @param args
	 *            command line arguments (ignored)
	 */
	public static void main(String args[]) {
		// Configuration of graph database - set the database path for which you
		// have to test
		db = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder(new File(
						"C:/Users/vin/Desktop/RIT Sem 4/GraphDatabases/Assignment4/db/proteins/backbones_1OVO"))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "512M")
				.setConfig(GraphDatabaseSettings.string_block_size, "60")
				.setConfig(GraphDatabaseSettings.array_block_size, "300").newGraphDatabase();
		System.out.println("For DB path: "+db.toString());
		//Uncomment or comment following line to run or not run proteins query graph by setting appropriate path
		transformProteinsFormat(new File(
				"C:/Users/vin/Desktop/RIT Sem 4/GraphDatabases/Assignment4/Proteins/Proteins/part3_Proteins/Proteins/query/mus_musculus_2LCK.16.sub.grf"));

		//Uncomment or comment following line to run or not run iGraph query graph by setting appropriate path
		//transformiGRAPHFormat(new File("C:/Users/vin/Desktop/RIT Sem 4/GraphDatabases/Assignment4/iGraph/iGraph/human_q10.igraph"));
		System.out.println("Vertices: "+verticesSet);
		System.out.println("Adjacency List: "+relationshipSet);
		
		System.out.println("-------Naive sub-graph matching results---------");
		Long start1 = System.currentTimeMillis();
		//calculating search space
		oldSearchSpace();
		int counter = 0;
		for (int d : verticesSet.keySet()) {
			finalOrder.put(counter, d);
			counter++;
		}
		//search initiation
		search(0);
		Long end1 = System.currentTimeMillis();
		System.out.println("Count: " + solutionSet.size());
		System.out.println("Time taken in seconds:" + ((end1 - start1) / 1000.0));

		tempSolution.clear();
		solutionSet.clear();
		ArrayList<Integer> nodes = new ArrayList<Integer>();
		ArrayList<Integer> order = new ArrayList<Integer>();
		int min = Integer.MAX_VALUE;
		int index = 0;
		System.out.println("-------GraphQL sub-graph matching results---------");
		Long start2 = System.currentTimeMillis();
		// Reducing search space by neighborhood profiling
		reduceSearchSpace();
		for (int j : searchSpace.keySet()) {
			if (searchSpace.get(j).size() < min) {
				min = searchSpace.get(j).size();
				index = j;
			}
		}
		//finding optimized query order by initiating it with node woth minimum search space
		try{
		order = findQueryOrder(nodes, index, order, 1);
		for (int i : searchSpace.keySet()) {
			if (!nodes.contains(i))
				order = findQueryOrder(nodes, i, order, 0);
		}
		System.out.println("Order: "+order);
		}catch(NullPointerException e){
			System.out.println("Order: No solution available");			
		}
		
		counter = 0;
		for (int d : verticesSet.keySet()) {
			finalOrder.put(counter, d);
			counter++;
		}
		search(0);
		System.out.println("Count: " + solutionSet.size());
		Long end2 = System.currentTimeMillis();
		System.out.println("Time taken in seconds:" + ((end2 - start2) / 1000.0));
	}


	/**
	 * The oldSearchSpace function to compute search space for naive subgraph matching
	 *
	 */
	private static void oldSearchSpace() {
		searchSpace.clear();
		Transaction tx = db.beginTx();
		for (Integer nodeCur : verticesSet.keySet()) {
			ResourceIterator<Node> itr = db.findNodes(Label.label(verticesSet.get(nodeCur)));
			while (itr.hasNext()) {
				Node n = itr.next();
				if (searchSpace.containsKey(nodeCur)) {
					searchSpace.get(nodeCur).add(n);
				} else {
					searchSpace.put(nodeCur, new ArrayList<Node>());
					searchSpace.get(nodeCur).add(n);
				}
			}
		}
		tx.success();
		System.out.println("Node:  SearchSpace");
		for (int i : searchSpace.keySet()) {
			System.out.println(i + ": " + searchSpace.get(i).size());
		}
	}

	/**
	 * The reduceSearchSpace function to reduce search space by neighborhood profiling
	 *
	 */
	private static void reduceSearchSpace() {
		searchSpace.clear();
		Transaction tx = db.beginTx();

		for (Integer nodeCur : verticesSet.keySet()) {
			// Updating the node with the neighborhood profile
			ArrayList<Label> profile = new ArrayList<Label>();
			for (int j : relationshipSet.get(nodeCur)) {
				profile.add(Label.label(verticesSet.get(j)));
			}
			//getting all the nodes with label match
			ResourceIterator<Node> itr = db.findNodes(Label.label(verticesSet.get(nodeCur)));
			while (itr.hasNext()) {
				boolean flag = true;
				Node n = itr.next();
				//getting all relationships for a particular node
				Iterator<Relationship> relationItr = n.getRelationships(Direction.OUTGOING).iterator();
				ArrayList<Label> check = new ArrayList<Label>();
				while (relationItr.hasNext()) {
					Node tempNode = relationItr.next().getOtherNode(n);
					check.add(tempNode.getLabels().iterator().next());
				}
				//checking whether the particular node has relationships which a node in a subgraph has. 
				for (Label l : profile) {
					if (check.contains(l)) {
						check.remove(l);
					} else {
						flag = false;
					}
				}
				//if all relationships were present then add the node in the search space.
				if (flag) {
					if (searchSpace.containsKey(nodeCur)) {
						searchSpace.get(nodeCur).add(n);
					} else {
						searchSpace.put(nodeCur, new ArrayList<Node>());
						searchSpace.get(nodeCur).add(n);
					}
				}
			}
		}
		tx.success();
		System.out.println("Node:  SearchSpace");
		for (int i : searchSpace.keySet()) {
			System.out.println(i + ": " + searchSpace.get(i).size());
		}
	}

	/**
	 * The search function recursively calls itself to match the subgraph
	 *
	 * @param i - integer representating current node
	 *       
	 */	
	private static void search(int i) {
		for (Node n : searchSpace.get(i)) {
			if (tempSolution.size() > i) {
				tempSolution.remove(i);
			}
			if (tempSolution.contains(n))
				continue;
			if (!check(tempSolution, n, i))
				continue;
			tempSolution.add(n);
			if (i < verticesSet.size() - 1) {
				search(i + 1);
			} else {
				if (tempSolution.size() == verticesSet.size()) {
					
					//for iGraph format requirement adding a constraint - Uncomment this code to add constraint. 
					//if(solutionSet.size()>1000){
					//	System.exit(0);
					//}
					System.out.println(Arrays.toString(tempSolution.toArray()));
					solutionSet.add(tempSolution);
				}
			}
		}
	}

	/**
	 * The check function checks whether current node satisfies relationships with nodes in the tempSolution if there are any.
	 *
	 * @param tempSolution - partial solution in ArrayList
	 * @param n - node n representating current node
	 * @param i - integer representating current search index
	 * 
	 * @return true if a current node satisfies relationships with nodes in the tempSolution if there are any otherwise false.
	 *       
	 */	
	private static boolean check(ArrayList<Node> tempSolution, Node n, int i) {
		int l = 0;
		if (!tempSolution.isEmpty()) {
			for (int k = 0; k < i; k++) {
				l = 0;
				if (relationshipSet.get(k).contains(i)) {
					Iterator<Relationship> itr = n.getRelationships(Direction.OUTGOING).iterator();
					while (itr.hasNext()) {
						Node tempNode = itr.next().getOtherNode(n);
						if (tempSolution.get(k).getId() == tempNode.getId()) {
							l = 1;
						}
					}
					if (l == 0) {
						return false;
					}
				}
			}
		}
		return true;
	}

	/**
	 * The findQueryOrder program finds the optimized query order which improves subgraph matching
	 *
	 * @param nodes - integer ArrayList to store covered nodes
	 * @param nodes - integer n to store current node
	 * @param nodes - integer ArrayList to store the partial order
	 * @param decision - integer to decide which value of gamma is to be used while computations. 
	 *            
	 */
	private static ArrayList<Integer> findQueryOrder(ArrayList<Integer> nodes, int n, ArrayList<Integer> order,
			int decision) {
		float gammaValue = 0.5f;
		int minVal = Integer.MAX_VALUE;
		int minIndex = 0;
		int count = 0;
		if (!nodes.contains(n)) {
			nodes.add(n);
			order.add(n);
			for (int k : order) {
				for (int i : relationshipSet.get(k)) {
					if (decision == 0) {
						for (int j : relationshipSet.get(i)) {
							if (order.contains(j))
								count++;
						}
						gammaValue = (float) Math.pow(gammaValue, count);
						if (((!nodes.contains(i)) && (minVal > (gammaValue * searchSpace.get(i).size())))) {
							minVal = (int) Math.abs(gammaValue * searchSpace.get(i).size());
							minIndex = i;
						}
					} else {
						if (((!nodes.contains(i)) && (minVal > (searchSpace.get(i).size())))) {
							minVal = (int) Math.abs(searchSpace.get(i).size());
							minIndex = i;
						}
					}
				}
			}
			minVal = minIndex;
			order = findQueryOrder(nodes, minVal, order, 0);
		}
		return order;
	}

	
	/**
	 * The transformProteinsFormat function transforms proteins format query and stores it
	 *
	 * @param File input: Input proteins file
	 *            
	 */
	private static void transformProteinsFormat(File file) {
		System.out.println("Proteins DB -->" + file.getName());
		BufferedReader br;
		String currentLine = "";
		int count = 0;
		try {
			br = new BufferedReader(new FileReader(file));
			String[] tempStr;
			int vertexCount = Integer.parseInt(br.readLine().trim());
			for (int j = 0; j < vertexCount; j++) {
				currentLine = br.readLine();
				if (currentLine != null && currentLine.charAt(0) != '#') {
					tempStr = currentLine.split(" ");
					verticesSet.put(Integer.parseInt(tempStr[0].trim()), tempStr[1].trim());
				}
			}

			while ((currentLine = br.readLine()) != null) {
				int limit = Integer.parseInt(currentLine.trim());
				for (int k = 0; k < limit; k++) {
					currentLine = br.readLine();
					String[] tempStr1 = currentLine.split(" ");
					if (!relationshipSet.containsKey(Integer.parseInt(tempStr1[0].trim()))) {
						relationshipSet.put(Integer.parseInt(tempStr1[0].trim()), new ArrayList<Integer>());
						relationshipSet.get(Integer.parseInt(tempStr1[0].trim()))
								.add(Integer.parseInt(tempStr1[1].trim()));
					} else {
						relationshipSet.get(Integer.parseInt(tempStr1[0].trim()))
								.add(Integer.parseInt(tempStr1[1].trim()));
					}
					count = count + 1;
				}
			}
			br.close();
			System.out.println("Vertices Count: " + verticesSet.size());
			System.out.println("Edges Count: " + count);
			System.out.println("Subgrap query stored");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * The transformiGRAPHFormat function transforms iGraph format query and stores it
	 *
	 * @param File input: Input iGraph file
	 *            
	 */
	private static void transformiGRAPHFormat(File file) {
		System.out.println("iGRAPH DB -->" + file.getName());
		BufferedReader br;
		String currentLine = "";
		int count = 0, count1 = 0;
		try {
			br = new BufferedReader(new FileReader(file));
			String[] tempStr;
			while ((currentLine = br.readLine()) != null) {
				tempStr = currentLine.split(" ");
				if (tempStr[0].trim().equals("v")) {
					Label[] labels = new Label[tempStr.length - 2];
					count = 0;
					for (int i = 2; i < tempStr.length; i++, count++) {
						labels[count] = Label.label(tempStr[i].trim());
					}
					verticesSet.put(Integer.parseInt(tempStr[1].trim()), tempStr[2].trim());
				} else if (tempStr[0].trim().equals("e")) {
					count1 = count1 + 1;
					if (!relationshipSet.containsKey(Integer.parseInt(tempStr[1].trim()))) {
						relationshipSet.put(Integer.parseInt(tempStr[1].trim()), new ArrayList<Integer>());
						relationshipSet.get(Integer.parseInt(tempStr[1].trim()))
								.add(Integer.parseInt(tempStr[2].trim()));
						
						if (!relationshipSet.containsKey(Integer.parseInt(tempStr[2].trim()))) {
							relationshipSet.put(Integer.parseInt(tempStr[2].trim()), new ArrayList<Integer>());
							relationshipSet.get(Integer.parseInt(tempStr[2].trim()))
									.add(Integer.parseInt(tempStr[1].trim()));
						}else{
					        if(!relationshipSet.get(Integer.parseInt(tempStr[2].trim())).contains(Integer.parseInt(tempStr[1].trim()))){
								relationshipSet.get(Integer.parseInt(tempStr[2].trim()))
								.add(Integer.parseInt(tempStr[1].trim()));					        	
					        }
						}
						
					} else {
						relationshipSet.get(Integer.parseInt(tempStr[1].trim()))
								.add(Integer.parseInt(tempStr[2].trim()));
						if (!relationshipSet.containsKey(Integer.parseInt(tempStr[2].trim()))) {
							relationshipSet.put(Integer.parseInt(tempStr[2].trim()), new ArrayList<Integer>());
							relationshipSet.get(Integer.parseInt(tempStr[2].trim()))
									.add(Integer.parseInt(tempStr[1].trim()));
						}else{
					        if(!relationshipSet.get(Integer.parseInt(tempStr[2].trim())).contains(Integer.parseInt(tempStr[1].trim()))){
								relationshipSet.get(Integer.parseInt(tempStr[2].trim()))
								.add(Integer.parseInt(tempStr[1].trim()));					        	
					        }
						}
						
					}
				}
				if (currentLine.contains("t") && count1 > 0) {
					break;
				}
			}
			br.close();
			System.out.println("Vertices Count: " + verticesSet.size() + " " + verticesSet);
			System.out.println("Edges Count: " + count1);
			System.out.println("Subgrap query stored");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
