
/* 
 * GraphQLTest.java 
 * 
 * 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.neo4j.unsafe.batchinsert.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * This GraphTransformTest class transforms formats of iGraph and proteins into Neo4j graph database
 * 
 * @author Vinay Vasant More
 *
 */

public class GraphTransformTest {

	/**
	 * The main program.
	 *
	 * @param args
	 *            command line arguments (ignored)
	 */
	public static void main(String args[]) {
		// Transforming human iGraph format
		File humanInput = new File(
				"C:/Users/vin/Desktop/RIT Sem 4/GraphDatabases/Assignment4/iGraph/iGraph/human.igraph");
		File humanDB = new File("C:/Users/vin/Desktop/RIT Sem 4/GraphDatabases/Assignment4/db/human");
		transformiGRAPHFormat(humanInput, humanDB);

		// Transforming yeast iGraph format
		File yeastInput = new File(
				"C:/Users/vin/Desktop/RIT Sem 4/GraphDatabases/Assignment4/iGraph/iGraph/yeast.igraph");
		File yeastDB = new File("C:/Users/vin/Desktop/RIT Sem 4/GraphDatabases/Assignment4/db/yeast");
		transformiGRAPHFormat(yeastInput, yeastDB);

		// Transforming proteins format
		File proteinsInput = new File(
				"C:/Users/vin/Desktop/RIT Sem 4/GraphDatabases/Assignment4/Proteins/Proteins/part3_Proteins/Proteins/target");
		String proteinsDB = "C:/Users/vin/Desktop/RIT Sem 4/GraphDatabases/Assignment4/db/proteins";
		transformProteinsFormat(proteinsInput, proteinsDB);
	}

	/**
	 * The transformiGRAPHFormat function transforms iGraph format
	 *
	 * @param File input: Input iGraph file
	 * @param File db: db file path
	 *            
	 */
	private static void transformiGRAPHFormat(File input, File db) {
		// TODO Auto-generated method stub
		System.out.println("Neo4j graphdb batch inserter initiated for iGraph");
		HashMap<Integer, Long> verticesSet = new HashMap<Integer, Long>();
		BufferedReader br;
		BatchInserter insert;
		String currentLine = "";
		int count = 0, count1 = 0;
		try {
			br = new BufferedReader(new FileReader(input));
			insert = BatchInserters.inserter(db);
			String[] tempStr;
			while ((currentLine = br.readLine()) != null) {
				tempStr = currentLine.split(" ");
				if (tempStr[0].trim().equals("v")) {
					Label[] labels = new Label[tempStr.length - 2];
					HashMap<String, Object> hs = new HashMap<String, Object>();
					hs.put("vertex", tempStr[1].trim());
					count = 0;
					for (int i = 2; i < tempStr.length; i++, count++) {
						labels[count] = Label.label(tempStr[i].trim());
					}
					verticesSet.put(Integer.parseInt(tempStr[1].trim()), insert.createNode(hs, labels));
				} else if (tempStr[0].trim().equals("e")) {
					count1 = count1 + 1;
					insert.createRelationship(verticesSet.get(Integer.parseInt(tempStr[1].trim())),
							verticesSet.get(Integer.parseInt(tempStr[2].trim())),
							RelationshipType.withName(tempStr[3].trim()), null);
				}
			}
			insert.shutdown();
			br.close();
			System.out.println("Vertices Count: " + verticesSet.size());
			System.out.println("Edges Count: " + count1);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * The transformProteinsFormat function transforms proteins format
	 *
	 * @param File input: Input porteins file
	 * @param String db: db file path
	 *            
	 */
	private static void transformProteinsFormat(File input, String db) {
		System.out.println("Neo4j graphdb batch inserter initiated for Proteins");
		for (File file : input.listFiles()) {
			System.out.println("Proteins DB -->"+file.getName());
			HashMap<Integer, Long> verticesSet = new HashMap<Integer, Long>();
			BufferedReader br;
			BatchInserter insert;
			String currentLine = "";
			int count = 0;
			try {
				br = new BufferedReader(new FileReader(file));
				insert = BatchInserters
						.inserter(new File(db + "/" + file.getName().substring(0, file.getName().length() - 4)));
				String[] tempStr;
				int vertexCount = Integer.parseInt(br.readLine().trim());
				for (int j = 0; j < vertexCount; j++) {
					currentLine = br.readLine();
					if (currentLine != null && currentLine.charAt(0) != '#') {
						tempStr = currentLine.split(" ");
						HashMap<String, Object> hs = new HashMap<String, Object>();
						hs.put("attribute", tempStr[1].trim());
						verticesSet.put(Integer.parseInt(tempStr[0].trim()),
								insert.createNode(hs, Label.label(tempStr[1].trim())));
					}
				}

				while ((currentLine = br.readLine()) != null) {
					int limit = Integer.parseInt(currentLine.trim());
					for (int k = 0; k < limit; k++) {
						currentLine = br.readLine();
						// System.out.println(currentLine);
						String[] tempStr1 = currentLine.split(" ");
						insert.createRelationship(verticesSet.get(Integer.parseInt(tempStr1[0].trim())),
								verticesSet.get(Integer.parseInt(tempStr1[1].trim())),
								RelationshipType.withName("edge"), null);
						count = count + 1;
					}
				}
				insert.shutdown();
				br.close();
				System.out.println("Vertices Count: " + verticesSet.size());
				System.out.println("Edges Count: " + count);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}