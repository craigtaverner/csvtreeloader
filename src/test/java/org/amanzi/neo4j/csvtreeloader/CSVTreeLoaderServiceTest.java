package org.amanzi.neo4j.csvtreeloader;

import org.amanzi.neo4j.csvtreeloader.CSVTreeBuilder.RootTreeNodeBuilder;
import org.amanzi.neo4j.csvtreeloader.CSVTreeBuilder.TreeNodeBuilder;
import org.apache.commons.collections.MapUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class CSVTreeLoaderServiceTest {

	private GraphDatabaseService db;
	private CSVTreeLoaderService service;
	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp() {
		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
		service = new CSVTreeLoaderService();
	}

	@After
	public void tearDown() throws Exception {
		db.shutdown();
	}
	
	@Test
	public void shouldGetValidInfo() throws IOException {
		Response response = service.info(true,db);
		System.out.println("Got Import response: " + response);
		JsonNode tree = objectMapper.readTree(response.getEntity().toString());
		String version = tree.get("version").asText();
		assertTrue("Version should be valid string", version.length() >= 5);
	}

	private String[] makeSampleCSV(String filename) throws IOException {
		String[] sample = new String[] {
				"DeviceID,Day,Time",
				"ABC,2014-03-20,2014-03-20T12:00:00",
				"ABC,2014-03-20,2014-03-20T12:30:00",
				"ABC,2014-03-21,2014-03-21T12:30:00",
				"ABC,2014-03-22,2014-03-22T12:30:00",
				"ABX,2014-03-20,2014-03-20T12:30:00",
				"ABX,2014-03-21,2014-03-21T12:30:00",
				"ABC,2014-03-20,2014-03-20T12:45:00",
				"ABC,2014-03-20,2014-03-20T12:00:00"
		};
		PrintWriter out = new PrintWriter(filename);
		for (String line : sample) {
			System.out.println(line);
			out.println(line);
		}
		out.close();
		return sample;
	}

	/**
	 * Check that all leaf nodes have the same expected path length from the
	 * root
	 * 
	 * @param rootLabel
	 *            the Label of the root nodes to begin search on
	 * @param pathLength
	 *            the expected length of the path to the leaf nodes
	 * @param expectedProperties
	 *            the expected properties all leaf nodes should have
	 * @param engine
	 */
	private void runLeafPropertiesCheck(String rootLabel, int pathLength, String[] expectedProperties,
			ExecutionEngine engine) {
		String query = "MATCH p=(n:" + rootLabel + ")-[:child*]->(e) WHERE NOT (e)-[:child]->() RETURN e,p";
		ExecutionResult results = engine.execute(query);
		ResourceIterator<Object> leaves = results.columnAs("e");
		ResourceIterator<Object> paths = results.columnAs("p");
		ACollections.IncMap<String> badPropCounts = new ACollections.IncMap<String>();
		int badPathLengths = 0;
		int leafCount = 0;
		while (leaves.hasNext()) {
			Node leaf = (Node) leaves.next();
			Path path = (Path) paths.next();
			Set<String> properties = new ACollections.HashSet<String>(leaf.getPropertyKeys());
			for (String property : expectedProperties) {
				if (!properties.contains(property)) {
					badPropCounts.inc(property);
				}
			}
			if (path.length() + 1 != pathLength) {
				System.out.println("Bad path: " + path);
				badPathLengths++;
			}
		}
		assertTrue("Found no leaf nodes", leafCount == 0);
		assertTrue("Found " + badPathLengths + " paths not matching length " + pathLength, badPathLengths == 0);
		if (badPropCounts.size() > 0) {
			String propMessage = "Found " + badPropCounts.size() + " properties missing from " + leafCount
					+ " leaf nodes";
			System.out.println(propMessage);
			for (String prop : badPropCounts.keySet()) {
				System.out
						.println("\t" + (prop + "                ").substring(0, 15) + "\t" + badPropCounts.get(prop));
			}
			assertTrue(propMessage, false);
		}
	}
	
	/**
	 * This method makes sure the graph is sane, with no duplicated days under
	 * the device nodes. As well as checking that all expected numbers of
	 * device-dates exist.
	 * 
	 * @param expectedDays
	 *            A Map<String,Integer> of deviceid to day count expected
	 * @param engine
	 *            The ExecutionEngine to perform the Cypher query with
	 * @param b 
	 */
	private void runDeviceDayCheck(String rootLabel, Map<String,Integer> expectedDays, ExecutionEngine engine) {
		String query = "MATCH (n:" + rootLabel + ")-[*]-(d:EventDay) RETURN n.deviceid as deviceid, d.day as day";
		ExecutionResult results = engine.execute(query);
		ACollections.IncMap<String> resultsMap = new ACollections.IncMap<String>();
		ACollections.IncMap<String> dayCounts = new ACollections.IncMap<String>();
		for (Map<String, Object> record : results) {
			String deviceid = (String) record.get("deviceid");
			String day = (String) record.get("day");
			String key = deviceid + "-" + day;
			resultsMap.inc(key);
			dayCounts.inc(deviceid);
		}
		assertTrue("Expected results, but got none: " + query, resultsMap.size() > 0);
		ArrayList<String> badResults = new ArrayList<String>();
		for (Map.Entry<String, Integer> counts : resultsMap.entrySet()) {
			//System.out.println("Have day count " + counts);
			if (counts.getValue() != 1) {
				badResults.add(counts.getKey() + "=" + counts.getValue());
			}
		}
		assertTrue("Not all device-days were unique: " + badResults, badResults.size() == 0);
		for (Map.Entry<String, Integer> entry : expectedDays.entrySet()) {
			String deviceid = entry.getKey();
			int count = entry.getValue();
			if (!dayCounts.containsKey(deviceid)) {
				assertTrue("Did not find expected entry " + deviceid, false);
			}
			assertEquals(count, (int) dayCounts.get(deviceid));
		}
	}
	
	/**
	 * This method tests each and every leaf node of the specified deviceid and
	 * day for the specified time fields
	 * 
	 * @param deviceid
	 * @param day
	 * @param times
	 * @param child 
	 * @param engine
	 */
	private void runEventMatchCheck(String deviceid, String day, String[] times, RelationshipType child,
			ExecutionEngine engine) {
		@SuppressWarnings("unchecked")
		Map<String, Object> parameters = MapUtils.putAll(
				new HashMap<String, Object>(), new String[] { "deviceid", deviceid, "day", day }
		);
		String query = "MATCH (n:DeviceID {deviceid: {deviceid}})-[:"+child+"]->(d:EventDay {day: {day}})-[:"+child+"]->(e:Event) "
				+ "RETURN n.deviceid as deviceid, e.time as time ORDER BY time ASC";
		System.out.println(query);
		ResourceIterator<Object> resultIterator = engine.execute(query, parameters).columnAs("time");
		ACollections.IncMap<String> resultsMap = new ACollections.IncMap<String>();
		while (resultIterator.hasNext()) {
			String result = (String) resultIterator.next();
			resultsMap.inc(result);
		}
		assertTrue("Expected results, but got none: " + query, resultsMap.size() > 0);
		for (String time : times) {
			Integer count = resultsMap.get(time);
			System.out.println("\t" + time + ": " + count);
			assertTrue("Count should not be null", count != null);
			assertEquals(1, (int)count);
		}
		HashSet<String> timesSet = new HashSet<String>();
		Collections.addAll(timesSet, times);
		for (Map.Entry<String, Integer> entry : resultsMap.entrySet()) {
			String time = entry.getKey();
			assertTrue("Found " + time + " but did not expect to for " + query, timesSet.contains(time));
		}
	}

	@Test
	public void shouldBuildCorrectBuilders() throws IOException {
		buildAndTestWith(null, "tree0");
	}
	
	@Test
	public void shouldBuildCorrectBuildersWithTreeSpec() throws IOException {
		buildAndTestWith(new String[] { "DeviceID-childx->EventDay-childx->Event" }, "tree0");
	}
	
	@Test
	public void shouldBuildCorrectBuildersWithNamedTreeSpec() throws IOException {
		buildAndTestWith(new String[] { "XYZ:DeviceID-child->EventDay-child->Event" }, "XYZ");
	}
	
	@Test
	public void shouldBuildCorrectBuildersWithMultipleNamedTreeSpec() throws IOException {
		buildAndTestWith(new String[] { "XYZ:DeviceID-child->EventDay-child->Event", "DayFirst:EventDay-childx->DeviceID-childx->Event" }, "XYZ");
	}
	
	private void buildAndTestWith(String[] treeSpecs, String testTree) throws IOException {
		CSVTreeLoaderService.verbose = true;
		String filename = "/tmp/csvtreeloaderservicetest.csv";
		String[] sample = makeSampleCSV(filename);
		String[] parentColumnHeaders = new String[] { "DeviceID", "Day..EventDay", "Time.time.Event" };
		String[] fullColumnHeaders = new String[] { "DeviceID.deviceid.DeviceID", "Day.day.EventDay", "Time.time.Event" };
		CSVTreeBuilder builder = new CSVTreeBuilder(filename, parentColumnHeaders, treeSpecs, null, null, db);
		builder.setLogger(System.out);
		List<TreeNodeBuilder> treeNodes = builder.treeStructures.get(testTree).builders;
		assertEquals(3, treeNodes.size());
		for (int i = 0; i < parentColumnHeaders.length; i++) {
			TreeNodeBuilder treeNode = treeNodes.get(i);
			String[] columnHeader = fullColumnHeaders[i].split("\\.");
			assertEquals(columnHeader[0], treeNode.column.name);
			assertEquals(columnHeader[1], treeNode.column.property);
			assertEquals(columnHeader[2], treeNode.column.label.toString());
			String[] className = treeNode.getClass().toString().split("[\\.\\$]");
			if (treeNode.parentBuilder == null) {
				assertEquals("RootTreeNodeBuilder", className[className.length - 1]);
				assertEquals(0, ((RootTreeNodeBuilder) treeNode).cachedRoots.size());
			} else {
				assertEquals("TreeNodeBuilder", className[className.length - 1]);
			}
		}
		// Now read the CSV file
		try (Transaction tx = db.beginTx()) {
			builder.read();
			builder.dumpAllTrees(0);
			// And test the contents of the builder now
			assertEquals(2, ((RootTreeNodeBuilder) treeNodes.get(0)).cachedRoots.size());
			assertEquals("ABC", treeNodes.get(0).currentNode.getProperty("deviceid"));
			String lastTime = sample[sample.length-1].split(",")[2];
			assertEquals(lastTime, treeNodes.get(2).currentNode.getProperty("time"));

			// And test the graph, first by checking the right number of root nodes
			ExecutionEngine engine = new ExecutionEngine(db);;
			ResourceIterator<Object> resultIterator = engine.execute("MATCH (n:DeviceId) RETURN n").columnAs("n");
			HashSet<String> expected = new HashSet<String>();
			expected.add("ABC");
			expected.add("ABX");
			while (resultIterator.hasNext()) {
				Node node = (Node) resultIterator.next();
				String name = node.getProperty("deviceid").toString();
				assertTrue("Node should have expected property", expected.contains(name));
			}

			// currently don't have right test for multiple overlapping trees
			// TODO: Write tests for this case, or make these test work
			if (treeSpecs == null || treeSpecs.length == 1) {
				// Now test that there are never duplicate date nodes, and that
				// days counts are correct
				RelationshipType child = treeNodes.get(0).childRel;
				if (treeSpecs != null) {
					assertTrue("Spec should contain correct relationship type",
							treeSpecs[0].contains("-" + child + "-"));
				}
				HashMap<String, Integer> expectedDays = new HashMap<String, Integer>();
				expectedDays.put("ABC", 3);
				expectedDays.put("ABX", 2);
				runDeviceDayCheck("DeviceID", expectedDays, engine);

				// Now test that the specific deviceid and date has the correct
				// number of leaf nodes
				runEventMatchCheck("ABX", "2014-03-20", new String[] { "2014-03-20T12:30:00" }, child, engine);
				runEventMatchCheck("ABC", "2014-03-20", new String[] { "2014-03-20T12:00:00", "2014-03-20T12:30:00",
						"2014-03-20T12:45:00" }, child, engine);
			}

			tx.success();
		}
	}

	private void testTreeGraph(String rootLabel, String[] leafProperties, Object... args) {
		HashMap<String, Integer> expectedDays = new HashMap<String, Integer>();
		for (int i = 0; i < args.length - 1; i += 2) {
			expectedDays.put((String) args[i], (Integer) args[i + 1]);
		}
		try (Transaction tx = db.beginTx()) {
			ExecutionEngine engine = new ExecutionEngine(db);
			// Now test that there are never duplicate date nodes, and that days
			// counts are correct
			runDeviceDayCheck(rootLabel, expectedDays, engine);
			runLeafPropertiesCheck(rootLabel, 3, leafProperties, engine);
			tx.success();
		}
	}

	private Response runDeviceConfigCSVImport(String path, String[] headers, String[] trees, String[] leaf, String leafProperties) {
		return runCSVImport(path, headers, trees, leaf, leafProperties, 0, 0, true);
	}
	
	//DeviceID        Device NodeID   Day     Day NodeID      Path    Date    UTC     Params
	private static final String[] OLDER_SPEC = new String[] { "DeviceID", "Day..EventDay", "Date.time.Event" };
	private static final String[] OLDER_LEAF = new String[] { "Date.time", "Path", "UTC" };
	private static final String[] OLDER_LEAF_PROPS = new String[] { "time", "utc", "path", "deviceid", "name", "version",
		"platform", "model", "os" };

	//Device	Version	Version Props	Day	Date	UTC	Params
	private static final String[] NEWER_SPEC = new String[] { "Device.deviceid", "Version.version_name.GeoptimaVersion.Version%20Props", "Day..EventDay", "Date.time.ConfigCheck" };
	private static final String[] NEWER_TREE = new String[] { "Device-versions->GeoptimaVersion-days->EventDay-checks->ConfigCheck" };
	private static final String[] NEWER_LEAF = new String[] { "Date.time", "UTC" };
	private static final String[] NEWER_LEAF_PROPS = new String[] { "deviceid", "mcc", "mnc", "eula", "lc"};

	//Device	Version	Version Props	Day	Date	UTC	Params
	private static final String[] VALIDATION_SPEC = new String[] {
		  "Device.deviceid..DeviceData",
		  "Version.version_name.GeoptimaVersion.VersionData",
		  "Day..EventDay",
		  "UploadFile.upload_file..FileData"
	};
	private static final String[] VALIDATION_TREE = new String[] {
	    "Device-versions->GeoptimaVersion-days->EventDay-uploads->UploadFile"		
	};
	private static final String[] VALIDATION_LEAF = new String[] { "Errors" };
	private static final String[] VALIDATION_LEAF_PROPS = new String[] { "first", "last", "upload_server", "upload_client", "carrier", "mcc", "mnc"};

	private Response runCSVImport(String path, String[] headers, String[] trees, String[] leaf, String leafProperties, long skip, long limit, boolean debug) {
		Response response = service.importCSV(
				path,
				new ACollections.ArrayList<String>(headers),
				new ACollections.ArrayList<String>(trees),
				new ACollections.ArrayList<String>(leaf),
				leafProperties,
				skip,
				limit,
				debug,
				db);
		System.out.println("Got Import response: " + response);
		return response;
	}
	
	@Test
	public void shouldImportFromOlderCSV() throws IOException {
		CSVTreeLoaderService.verbose = false;
		Response response = runDeviceConfigCSVImport("samples/353333333333333.csv", OLDER_SPEC, null, OLDER_LEAF, "Params");
		JsonNode tree = objectMapper.readTree(response.getEntity().toString());
		testTreeGraph("DeviceID", OLDER_LEAF_PROPS, "353333333333333", 3);
		int count = tree.get("count").asInt();
		assertEquals(122, count);
	}

	@Test
	public void shouldImportFromNewerCSV() throws IOException {
		CSVTreeLoaderService.verbose = false;
		Response response = runDeviceConfigCSVImport("samples/353222222222222.csv", NEWER_SPEC, NEWER_TREE, NEWER_LEAF, "Params");
		JsonNode tree = objectMapper.readTree(response.getEntity().toString());
		testTreeGraph("Device", NEWER_LEAF_PROPS, "353222222222222", 6);
		int count = tree.get("count").asInt();
		assertEquals(93, count);
	}

	@Test
	public void shouldImportFromValidationCSV() throws IOException {
		CSVTreeLoaderService.verbose = false;
		Response response = runDeviceConfigCSVImport("samples/validation.csv", VALIDATION_SPEC, VALIDATION_TREE, VALIDATION_LEAF, null);
		JsonNode tree = objectMapper.readTree(response.getEntity().toString());
		testTreeGraph("Device", VALIDATION_LEAF_PROPS, "350000000000010", 2, "350000000000013", 2);
		int count = tree.get("count").asInt();
		assertEquals(96, count);
	}

//	@Test
	public void shouldImportLargeCSV() throws IOException {
		//CSVTreeLoaderService.verbose = true;
		long start = System.currentTimeMillis();
		Response response = runCSVImport("samples/load_config_access.csv", OLDER_SPEC, null, OLDER_LEAF, "Params", 0, 0, true);
		System.out.println("Imported in " + (System.currentTimeMillis() - start) + "ms");
		JsonNode tree = objectMapper.readTree(response.getEntity().toString());
		testTreeGraph("DeviceID", OLDER_LEAF_PROPS, "358086051664420", 3, "358086051664420", 1);
		int count = tree.get("count").asInt();
		System.out.println("Finished large CSV test in " + (System.currentTimeMillis() - start) + "ms");
		assertEquals(367823, count);
	}

	public GraphDatabaseService graphdb() {
		return db;
	}
}
