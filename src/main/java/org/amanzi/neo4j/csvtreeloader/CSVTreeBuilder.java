package org.amanzi.neo4j.csvtreeloader;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;

/**
 * The tree builder class is responsible for managing the construction of
 * all trees based on the CSV file. The specification of the tree is
 * provided as an array of strings, each representing a column in the CSV
 * file to use as a level in the tree. The first column specified will be
 * the root nodes, and these are added to an index. The last column
 * specified will be the leaf nodes in the tree. Each column specifier is a
 * '.' separated string with 1, 2 or 3 possible components:
 * <dl>
 * <dt>Column Header</dt>
 * <dd>The column header needs to be an exact match for the value in the
 * first row of the CSV file.</dd>
 * <dt>Property Name</dt>
 * <dd>The property name is to be used to specify what key to use when
 * storing the values in the node. If the property is not specified, it is
 * calculated as the lower-case version of the column header with spaces
 * replaced by underscores.</dd>
 * <dt>Label</dt>
 * <dd>The label to use for the Nodes created at this level. If no label is
 * given, it is deduced from the column header with spaces removed.</dd>
 * </dl>
 * For example, the following CSV example:
 * 
 * <pre>
 * DeviceID,Day,Date
 * ABC,2014-03-20,2014-03-20T12:00:00
 * ABC,2014-03-20,2014-03-20T12:30:00
 * ABC,2014-03-21,2014-03-21T12:30:00
 * ABC,2014-03-22,2014-03-21T12:30:00
 * </pre>
 * 
 * could be loaded with a call to:
 * 
 * <pre>
 * /loadcsvtree/sample.csv?header=DeviceID&header=Day..EventDay&header=Date.time.Event
 * </pre>
 * 
 * and this would load a tree of three levels with one node in the first
 * level, two in the second and two under each of those (four events in
 * total). The tree would be searchable with a query like:
 * 
 * <pre>
 * MATCH (n:DeviceID {deviceid: {deviceid})-[:child]->(d:EventDay {day: {day}})-[:child]->(e:Event) RETURN n.deviceid,e.time
 * </pre>
 * 
 * Finding all events for a specific device and day if you pass in the
 * deviceid and day parameters.
 * 
 * @author craig
 * 
 */
class CSVTreeBuilder {
	private CSVReader csvReader;
	private long skip = 0;
	private long limit = 0;
	ArrayList<TreeNodeBuilder> treeNodes = new ArrayList<TreeNodeBuilder>();
	private PrintStream out;
	private GraphDatabaseService db;

	public CSVTreeBuilder(String file, String[] columnHeaderArray, List<String> leafProperties,
			String leafPropertiesColumn, GraphDatabaseService db) throws IOException {
		this.csvReader = new CSVReader(file);
		ArrayList<String> columnHeaders = new ArrayList<String>();
		Collections.addAll(columnHeaders, columnHeaderArray);
		initializeColumns(columnHeaders, leafProperties, leafPropertiesColumn, db);
	}

	public CSVTreeBuilder(String file, List<String> columnHeaders, List<String> leafProperties,
			String leafPropertiesColumn, GraphDatabaseService db) throws IOException {
		this.csvReader = new CSVReader(file);
		initializeColumns(columnHeaders, leafProperties, leafPropertiesColumn, db);
	}

	private void initializeColumns(List<String> columnHeaders, List<String> leafProperties,
			String leafPropertiesColumn, GraphDatabaseService db) {
		TreeNodeBuilder lastBuilder = null;
		this.db = db;
		for (String columnHeader : columnHeaders) {
			TreeNodeBuilder nodeBuilder = TreeNodeBuilder.makeFor(columnHeader, lastBuilder, db);
			treeNodes.add(nodeBuilder);
			lastBuilder = nodeBuilder;
		}
		if (lastBuilder != null) {
			lastBuilder.addProperties(leafProperties, leafPropertiesColumn);
		}
	}

	public void setPage(Long skip, Long limit) {
		if (skip != null) {
			this.skip = skip;
		}
		if (limit != null) {
			this.limit = limit;
		}
	}

	public void setLogger(PrintStream out) {
		this.out = out;
	}

	private void dumpTree(Node node, TreeNodeBuilder builder, int level) {
		try (Transaction tx = db.beginTx()) {
			String prefix = "    ";
			for (int i = 0; i < level; i++) {
				prefix += "    ";
			}
			out.println(prefix + node.getProperty(builder.column.property));
			for (Relationship rel : node
					.getRelationships(Direction.OUTGOING, DynamicRelationshipType.withName("child"))) {
				dumpTree(rel.getEndNode(), builder.childBuilder, level + 1);
			}
			tx.success();
		}
	}

	public void dumpTrees(int maxRoots) {
		if (out != null) {
			RootTreeNodeBuilder rootBuilder = (RootTreeNodeBuilder)this.treeNodes.get(0);
			int count = 0;
			System.out.println("Displaying tree graphs for " + (maxRoots > 0 ? maxRoots : "all") + " of "
					+ rootBuilder.cachedRoots.size() + " known roots");
			for(Entry<String, Node> entry: rootBuilder.cachedRoots.entrySet()) {
				if(maxRoots> 0 && count > maxRoots) {
					break;
				}
				String name = entry.getKey();
				Node root = entry.getValue();
				out.println("Dumping tree for "+name);
				dumpTree(root, rootBuilder, 0);
				count ++;
			}
			out.println();
		}
	}

	private long logIf(long start, long prevLog, long count) {
		if (out != null) {
			long now = System.currentTimeMillis();
			if (now - prevLog > 1000) {
				long rate = 1000 * count / (now - start);
				out.println("[" + (now - start) + "ms]: Imported " + count + " records (" + rate
						+ " records/second)");
				prevLog = now;
			}
		}
		return prevLog;
	}
	
	public long read() throws IOException {
		Map<String, String> record;
		long count = 0;
		long start = System.currentTimeMillis();
		long prevLog = start;
		if (treeNodes.size() > 0) {
			GraphDatabaseService db = treeNodes.get(0).db;
			Transaction tx = db.beginTx();
			try {
				while ((record = csvReader.readRecord()) != null) {
					if (limit > 0 && count >= (skip + limit)) {
						break;
					}
					if (skip <= 0 || count >= skip) {
						for (TreeNodeBuilder builder : treeNodes) {
							builder.addRecord(record);
						}
						prevLog = logIf(start, prevLog, count);
					}
					count++;
					if (count % 10000 == 0) {
						System.out.println("\t*** Commiting transaction ***");
						tx.success();
						tx.close();
						tx = db.beginTx();
					}
				}
				tx.success();
			} finally {
				tx.close();
			}
			logIf(start, 0, count);
		} else {
			System.err.println("Cannot import without columns defined");
		}
		return count;
	}
	
	static class ColumnSpec {
		String name;
		String property;
		Label label;
		public ColumnSpec(String columnSpec) {
			String[] headerSpec = columnSpec.split("\\.");
			this.name = headerSpec[0];
			this.property =
					(headerSpec.length > 1 && headerSpec[1].length() > 0) ? headerSpec[1] : this.name
							.toLowerCase().replaceAll("\\s+", "_");
			String labelName = (headerSpec.length > 2 && headerSpec[2].length() > 0) ? headerSpec[2] : StringUtils
					.capitalize(this.name).replaceAll("\\s+", "");
			this.label = DynamicLabel.label(labelName);
		}
	}

	/**
	 * The builder for the normal tree nodes are connected as 'child' nodes of a
	 * parent node. This allows for them to be found by traversing from the
	 * parent without the need for an index.
	 */
	static class TreeNodeBuilder {
		protected ColumnSpec column;
		protected TreeNodeBuilder parentBuilder;
		protected TreeNodeBuilder childBuilder;
		protected Node currentNode;
		protected HashMap<String,Node> currentChildren = new HashMap<String,Node>();
		public static final RelationshipType childRelType = DynamicRelationshipType.withName("child");
		protected GraphDatabaseService db;
		private List<ColumnSpec> properties = new ArrayList<ColumnSpec>();
		private String propertiesColumn;

		public static TreeNodeBuilder makeFor(String columnSpec, TreeNodeBuilder parentBuilder, GraphDatabaseService db) {
			if (parentBuilder == null) {
				return new RootTreeNodeBuilder(columnSpec, db);
			} else {
				return new TreeNodeBuilder(columnSpec, parentBuilder);
			}
		}

		public void addProperties(List<String> propertiesSpecs, String propertiesColumn) {
			if (propertiesSpecs != null) {
				for (String propSpec : propertiesSpecs) {
					this.properties.add(new ColumnSpec(propSpec));
				}
			}
			this.propertiesColumn = propertiesColumn;
		}

		private TreeNodeBuilder(String columnHeader, TreeNodeBuilder parentBuilder) {
			this.column = new ColumnSpec(columnHeader);
			if (parentBuilder != null) {
				this.db = parentBuilder.db;
				this.parentBuilder = parentBuilder;
				this.parentBuilder.childBuilder = this;
			}
		}

		protected void clearCurrentNode() {
			if(this.childBuilder!=null) {
				this.childBuilder.clearCurrentNode();
			}
			this.currentChildren.clear();
			this.currentNode = null;
		}

		protected Node findChild(String childProperty, String propertyValue) {
			if (currentChildren.size() == 0) {
				for (Relationship rel : currentNode.getRelationships(Direction.OUTGOING, childRelType)) {
					Node child = rel.getEndNode();
					currentChildren.put((String) child.getProperty(childProperty), child);
				}
			}
			return currentChildren.get(propertyValue);
		}

		protected Node addChild(Node child, String propertyValue) {
			return currentChildren.put(propertyValue, child);
		}

		protected void addPropertiesColumnToNode(Node node, String columnContent) {
			try {
				JsonNode tree = new ObjectMapper().readTree(columnContent);
				Iterator<String> fieldNames = tree.getFieldNames();
				while (fieldNames.hasNext()) {
					String name = fieldNames.next();
					node.setProperty(name, tree.get(name).toString());
				}
			} catch (JsonProcessingException e) {
				System.out.println("Failed to parse properties column as JSON: " + e);
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Error parsing properties column: " + e);
				e.printStackTrace();
			}
		}
		
		protected Node findOrMakeNode(String propertyValue, Map<String, String> record) {
			Node node = (parentBuilder != null) ? parentBuilder.findChild(column.property, propertyValue) : null;
			if (node == null) {
				node = db.createNode(column.label);
				node.setProperty(column.property, propertyValue);
				for (ColumnSpec prop : properties) {
					node.setProperty(prop.property, record.get(prop.name));
				}
				if (this.propertiesColumn != null) {
					addPropertiesColumnToNode(node, record.get(this.propertiesColumn));
				}
				if (parentBuilder != null) {
					parentBuilder.currentNode.createRelationshipTo(node, childRelType);
				}
			}
			return node;
		}

		/**
		 * This method should only be called after the parent builders addRecord
		 * method because it assumes that caches in the parent builder are
		 * up-to-date. In particular currentNode and currentChildren cannot have
		 * incorrect entries.
		 */
		protected final Node addRecord(Map<String, String> record) {
			String propertyValue = record.get(this.column.name);
			// We do not need to check the parent nodes, because they are
			// already checked, and currentNode set to null if parents change.
			if (currentNode == null || !currentNode.getProperty(column.property).equals(propertyValue)) {
				clearCurrentNode();	// clear child caches if we change current node
				if(CSVTreeLoaderService.verbose) {
					System.out.println("Creating new node (n:" + column.label + " {" + column.property + ": \"" + propertyValue + "\"})");
				}
				currentNode = findOrMakeNode(propertyValue, record);
			}
			return currentNode;
		}
	}

	/**
	 * The builder for the root node of the tree cannot be found by traversals,
	 * and so must be stored in a unique index based on the main property and
	 * value. We use the Neo4j 2.0 label-based schema indexes.
	 */
	public static class RootTreeNodeBuilder extends TreeNodeBuilder {
		protected ExecutionEngine engine;
		private String queryString;
		private Map<String, Object> parameters = new HashMap<String, Object>();
		protected HashMap<String, Node> cachedRoots = new HashMap<String, Node>();

		private RootTreeNodeBuilder(String columnSpec, GraphDatabaseService db) {
			super(columnSpec, null);
			this.db = db;
			try (Transaction tx = db.beginTx()) {
				boolean indexExists = false;
				for (IndexDefinition index : db.schema().getIndexes(this.column.label)) {
					ArrayList<String> keys = new ArrayList<String>();
					for (String key : index.getPropertyKeys()) {
						keys.add(key);
					}
					if (keys.size() != 1 || !keys.get(0).equals(this.column.property)) {
						throw new RuntimeException("Schema Index for " + this.column.label + "." + this.column.property
								+ " cannot be made because different index for " + this.column.label + "." + keys
								+ " already exists");
					}
					indexExists = true;
				}
				if (!indexExists) {
					db.schema().constraintFor(this.column.label).assertPropertyIsUnique(this.column.property).create();
				}
				tx.success();
			}
			this.engine = new ExecutionEngine(db);
			queryString = "MERGE (n:" + column.label + " {" + column.property + ": {" + column.property + "}}) RETURN n";
			try (Transaction tx = db.beginTx()) {
				ResourceIterator<Object> resultIterator = engine.execute("MATCH (n:" + column.label + ") RETURN n").columnAs(
						"n");
				while (resultIterator.hasNext()) {
					Node node = (Node) resultIterator.next();
					cachedRoots.put(node.getProperty(column.property).toString(), node);
				}
				tx.success();
				System.out.println("Cached " + cachedRoots.size() + " existing tree roots with Label '" + this.column.label
						+ "'");
			}
		}

		/**
		 * This version of the findOrMakeNode method only uses the index (or
		 * cache) to find the nodes, because they are roots of their own trees.
		 */
		protected Node findOrMakeNode(String propertyValue, Map<String, String> record) {
			// First look in the cache
			Node node = cachedRoots.get(propertyValue);
			if (node == null) {
				// Then use the schema index to find the node
				parameters.put(this.column.property, propertyValue);
				ResourceIterator<Object> resultIterator = engine.execute(queryString, parameters).columnAs("n");
				node = (Node) resultIterator.next();
				if (node == null) {
					// Finally make a new node if none found
					node = super.findOrMakeNode(propertyValue, record);
				}
				cachedRoots.put(propertyValue, node);
			}
			return node;
		}
	}

}
