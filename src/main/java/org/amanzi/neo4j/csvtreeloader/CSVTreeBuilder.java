package org.amanzi.neo4j.csvtreeloader;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
	private ArrayList<ColumnSpec> columns = new ArrayList<ColumnSpec>();
	Map<String, ColumnSpec> columnMap = new HashMap<String, ColumnSpec>();
	Map<String, TreeStructure> treeStructures = new HashMap<String, TreeStructure>();
	private PrintStream out;
	private GraphDatabaseService db;
	private ExecutionEngine engine;

	public CSVTreeBuilder(String file, String[] columnHeaderArray, String[] treesArray, List<String> leafProperties,
			String leafPropertiesColumn, GraphDatabaseService db) throws IOException {
		this.csvReader = new CSVReader(file);
		ArrayList<String> columnHeaders = new ArrayList<String>();
		Collections.addAll(columnHeaders, columnHeaderArray);
		ArrayList<String> trees = new ArrayList<String>();
		if (treesArray != null) {
			Collections.addAll(trees, treesArray);
		}
		initializeColumns(columnHeaders, trees, leafProperties, leafPropertiesColumn, db);
	}

	public CSVTreeBuilder(String file, List<String> columnHeaders, List<String> trees, List<String> leafProperties,
			String leafPropertiesColumn, GraphDatabaseService db) throws IOException {
		this.csvReader = new CSVReader(file);
		initializeColumns(columnHeaders, trees, leafProperties, leafPropertiesColumn, db);
	}

	private String makeUniqueTreeName() {
		for (int i = 0; i < 100; i++) {
			String name = "tree" + i;
			if (!treeStructures.containsKey(name)) {
				return name;
			}
		}
		throw new RuntimeException("Failed to make a new unique name - too many trees defined");
	}

	private void initializeColumns(List<String> columnHeaders, List<String> trees, List<String> leafProperties,
			String leafPropertiesColumn, GraphDatabaseService db) throws UnsupportedEncodingException {
		this.db = db;
		this.engine = new ExecutionEngine(db);
		for (String columnHeader : columnHeaders) {
			ColumnSpec column = new ColumnSpec(columnHeader);
			columns.add(column);
			columnMap.put(column.label.name(), column);
		}
		if (trees == null || trees.size() == 0) {
			TreeStructure tree = new TreeStructure(makeUniqueTreeName(), this.db, this.engine);
			tree.initializeWith(columns, leafProperties, leafPropertiesColumn);
			treeStructures.put(tree.name, tree);
		} else {
			for (String treeSpec : trees) {
				String treeName = null;
				String[] ts = treeSpec.split(":");
				if(ts.length>1){
					treeName = ts[0];
					treeSpec = ts[1];
				}else{
					treeName = makeUniqueTreeName();
				}
				TreeStructure tree = new TreeStructure(treeName, this.db, this.engine);
				tree.initializeWith(treeSpec, columnMap, leafProperties, leafPropertiesColumn);
				treeStructures.put(tree.name, tree);
			}
		}
		for(TreeStructure tree:treeStructures.values()){
			for(TreeStructure other:treeStructures.values()){
				tree.addRoots(other.getRootBuilder());
			}
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

	public void dumpAllTrees(int maxRoots) {
		if (out != null) {
			for (TreeStructure tree : treeStructures.values()) {
				tree.dumpTrees(this.out, tree.name, tree.builders, maxRoots);
			}
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

	public boolean isValid() {
		return (treeStructures.size() > 0 && treeStructures.values().toArray().length > 0);
	}
	
	public long read() throws IOException {
		Map<String, String> record;
		long count = 0;
		long start = System.currentTimeMillis();
		long prevLog = start;
		if (isValid()) {
			Transaction tx = db.beginTx();
			try {
				while ((record = csvReader.readRecord()) != null) {
					if (limit > 0 && count >= (skip + limit)) {
						break;
					}
					if (skip <= 0 || count >= skip) {
						for (TreeStructure tree : treeStructures.values()) {
							for (TreeNodeBuilder builder : tree.builders) {
								builder.addRecord(record);
							}
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
	
	static class TreeStructure {
		String name;
		List<TreeNodeBuilder> builders = new ArrayList<TreeNodeBuilder>();
		GraphDatabaseService db;
		ExecutionEngine engine;
		public static final DynamicRelationshipType DEFAULT_CHILD = DynamicRelationshipType.withName("child");

		/**
		 * Create a tree structure containing a list of levels to the tree
		 * @param name
		 * @param db
		 * @param engine
		 */
		public TreeStructure(String name, GraphDatabaseService db, ExecutionEngine engine) {
			this.name = name;
			this.db = db;
			this.engine = engine;
		}
		/**
		 * Tell this tree about other tree roots, so it can cross reference overlapping trees with constraints
		 * @param rootTreeNodeBuilder
		 */
		public void addRoots(RootTreeNodeBuilder rootTreeNodeBuilder) {
			for(TreeNodeBuilder builder:builders) {
				builder.considerQuivalentRoot(rootTreeNodeBuilder);
			}
		}
		public RootTreeNodeBuilder getRootBuilder(){
			return (RootTreeNodeBuilder) builders.get(0);
		}
		public void initializeWith(List<ColumnSpec> columns, List<String> leafProperties, String leafPropertiesColumn) throws UnsupportedEncodingException {
			TreeNodeBuilder parentBuilder = null;
			for (ColumnSpec column : columns) {
				TreeNodeBuilder builder = TreeNodeBuilder.makeFor(column, parentBuilder, DEFAULT_CHILD, db, engine);
				this.builders.add(builder);
				parentBuilder = builder;
			}
			if (parentBuilder != null) {
				parentBuilder.addProperties(leafProperties, leafPropertiesColumn);
			}
		}
		public void initializeWith(String treeSpec, Map<String,ColumnSpec> columns, List<String> leafProperties, String leafPropertiesColumn) throws UnsupportedEncodingException {
			TreeNodeBuilder parentBuilder = null;
			for (String columnSpec : treeSpec.split("->")) {
				String[] cs = columnSpec.split("-");
				String columnLabel = cs[0];
				RelationshipType child = cs.length > 1 ? DynamicRelationshipType.withName(URLDecoder.decode(cs[1],"US-ASCII")) : DEFAULT_CHILD;
				ColumnSpec column = columns.get(columnLabel);
				if (column == null) {
					throw new RuntimeException("Invalid tree '" + name + "': no such column '" + columnLabel + "'");
				} else {
					TreeNodeBuilder builder = TreeNodeBuilder.makeFor(column, parentBuilder, child, db, engine);
					this.builders.add(builder);
					parentBuilder = builder;
				}
			}
			if (parentBuilder != null) {
				parentBuilder.addProperties(leafProperties, leafPropertiesColumn);
			}
		}
		private void dumpTree(PrintStream out, Node node, TreeNodeBuilder builder, int level) {
			try (Transaction tx = db.beginTx()) {
				String prefix = "    ";
				for (int i = 0; i < level; i++) {
					prefix += "    ";
				}
				Iterator<Label> labelIterator = node.getLabels().iterator();
				String label = labelIterator.hasNext() ? labelIterator.next().name() : "Node";
				out.println(prefix + label + "[" + node.getId() + "]" + ": "
						+ node.getProperty(builder.column.property));
				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING, builder.childRel)) {
					dumpTree(out, rel.getEndNode(), builder.childBuilder, level + 1);
				}
				tx.success();
			}
		}

		public void dumpTrees(PrintStream out, String tree, List<TreeNodeBuilder> treeNodes, int maxRoots) {
			if (out != null) {
				RootTreeNodeBuilder rootBuilder = (RootTreeNodeBuilder) treeNodes.get(0);
				int count = 0;
				int knownRoots = rootBuilder.cachedRoots.size();
				if (maxRoots == 0) {
					maxRoots = knownRoots;
				}
				System.out.println("Displaying " + tree + " graphs for " + (maxRoots >= knownRoots ? "all" : maxRoots)
						+ " of " + knownRoots + " known roots");
				for (Entry<String, Node> entry : rootBuilder.cachedRoots.entrySet()) {
					if (count > maxRoots) {
						break;
					}
					String name = entry.getKey();
					Node root = entry.getValue();
					out.println("Dumping tree for " + name);
					dumpTree(out, root, rootBuilder, 0);
					count++;
				}
				out.println();
			}
		}
	}
	
	static class ColumnSpec {
		String name;
		String property;
		String properties;
		Label label;

		public ColumnSpec(String columnSpec) throws UnsupportedEncodingException {
			String[] headerSpec = columnSpec.split("\\.");
			this.name = URLDecoder.decode(headerSpec[0],"US-ASCII");
			this.property = field(headerSpec, 1, this.name.toLowerCase().replaceAll("\\s+", "_"));
			String defaultLabelName = StringUtils.capitalize(this.name).replaceAll("\\s+", "");
			this.label = DynamicLabel.label(field(headerSpec, 2, defaultLabelName));
			this.properties = field(headerSpec, 3, null);
		}

		private String field(String[] headerSpec, int index, String defaultValue) throws UnsupportedEncodingException {
			return (headerSpec.length > index && headerSpec[index].length() > 0) ? URLDecoder.decode(headerSpec[index],"US-ASCII") : defaultValue;
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
		private RootTreeNodeBuilder equivalentRootBuilder;
		protected Node currentNode;
		protected HashSet<String> mergedChildren = new HashSet<String>();
		protected GraphDatabaseService db;
		private List<ColumnSpec> properties = new ArrayList<ColumnSpec>();
		private String propertiesColumn;
		RelationshipType childRel = TreeStructure.DEFAULT_CHILD;

		public static TreeNodeBuilder makeFor(ColumnSpec columnSpec, TreeNodeBuilder parentBuilder, RelationshipType child, GraphDatabaseService db, ExecutionEngine engine) {
			if (parentBuilder == null) {
				return new RootTreeNodeBuilder(columnSpec, child, db, engine);
			} else {
				return new TreeNodeBuilder(columnSpec, parentBuilder, child);
			}
		}

		public void addProperties(List<String> propertiesSpecs, String propertiesColumn) throws UnsupportedEncodingException {
			if (propertiesSpecs != null) {
				for (String propSpec : propertiesSpecs) {
					this.properties.add(new ColumnSpec(propSpec));
				}
			}
			this.propertiesColumn = propertiesColumn;
		}

		private TreeNodeBuilder(ColumnSpec column, TreeNodeBuilder parentBuilder, RelationshipType child) {
			this.column = column;
			this.childRel = child;
			if (parentBuilder != null) {
				this.db = parentBuilder.db;
				this.parentBuilder = parentBuilder;
				this.parentBuilder.childBuilder = this;
			}
		}

		protected String getKey() {
			return this.column.label + "." + this.column.property;
		}
		
		public String toString() {
			return getKey();
		}
		
		protected void considerQuivalentRoot(RootTreeNodeBuilder root) {
			if (this != root && root.getKey().equals(this.getKey())) {
				this.equivalentRootBuilder = root;
				System.out.println("TreeNodeBuilder[" + this + "]: equivalent to root builder '" + root + "'");
			}
		}
		
		protected void clearCurrentNode() {
			if (this.childBuilder != null) {
				this.childBuilder.clearCurrentNode();
			}
			this.mergedChildren.clear();
			this.currentNode = null;
		}

		protected Node findChild(String childProperty, String propertyValue) {
			for (Relationship rel : currentNode.getRelationships(Direction.OUTGOING, childRel)) {
				Node child = rel.getEndNode();
				if (((String) child.getProperty(childProperty)).equals(propertyValue)) {
					return child;
				}
			}
			return null;
		}

		protected void addChild(Node child, String propertyValue) {
			currentNode.createRelationshipTo(child, childRel);
		}

		protected void addPropertiesColumnToNode(Node node, Map<String, String> record, String columnName) {
			String columnContent = record.get(columnName);
			try {
				if (columnContent != null) {
					JsonNode tree = new ObjectMapper().readTree(columnContent.replaceAll("\\=\\>", ":"));
					Iterator<String> fieldNames = tree.getFieldNames();
					while (fieldNames.hasNext()) {
						String name = fieldNames.next();
						setStringProperty(node, name, tree.get(name).toString());
					}
				} else {
					String errMessage = "No properties column found for '" + columnName + "'";
					CSVTreeLoaderService.logger.severe(errMessage);
					throw new IOException("Invalid column specification: "+errMessage);
				}
			} catch (JsonProcessingException e) {
				System.out.println("Failed to parse properties column '" + columnContent + "' as JSON: " + e);
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Error parsing properties column '" + columnContent + "': " + e);
				e.printStackTrace();
			}
		}

		private void setStringProperty(Node node, String key, String value) {
			if (value != null) {
				if (value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
					value = value.substring(1, value.length() - 1);
				}
				node.setProperty(key, value);
			}
		}

		protected Node findNode(String propertyValue, Map<String, String> record) {
			Node node = null;
			if (equivalentRootBuilder != null) {
				node = equivalentRootBuilder.findOrMakeNode(propertyValue, record);
			}
			if (parentBuilder != null) {
				if (node == null) {
					node = parentBuilder.findChild(column.property, propertyValue);
				} else {
					if (parentBuilder.findChild(column.property, propertyValue) == null) {
						parentBuilder.addChild(node, propertyValue);
					}
				}
			}
			return node;
		}
		
		protected Node findOrMakeNode(String propertyValue, Map<String, String> record) {
			Node node = findNode(propertyValue, record);
			boolean shouldMerge = !mergedChildren.contains(propertyValue);
			if (node == null) {
				node = db.createNode(column.label);
				setStringProperty(node, column.property, propertyValue);
				if (parentBuilder != null) {
					parentBuilder.addChild(node, propertyValue);
				}
				shouldMerge = true;
			}
			if (shouldMerge) {
				for (ColumnSpec prop : properties) {
					setStringProperty(node, prop.property, record.get(prop.name));
				}
				// Add all properties in the column spec for a multi-property column
				if (this.column.properties != null) {
					addPropertiesColumnToNode(node, record, this.column.properties);
				}
				// Add all properties in the leaf-specific column spec
				if (this.propertiesColumn != null) {
					addPropertiesColumnToNode(node, record, this.propertiesColumn);
				}
				mergedChildren.add(propertyValue);
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
				currentNode = findOrMakeNode(propertyValue, record);
				if (CSVTreeLoaderService.verbose) {
					System.out.println("Got " + currentNode + " (n:" + column.label + " {" + column.property + ": \""
							+ propertyValue + "\"})");
				}
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

		private RootTreeNodeBuilder(ColumnSpec columnSpec, RelationshipType child, GraphDatabaseService db, ExecutionEngine engine) {
			super(columnSpec, null, child);
			this.db = db;
			this.engine = engine;
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
