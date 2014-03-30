package org.amanzi.neo4j.csvtreeloader;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.List;
import java.util.HashMap;

/**
 * It is common that CSV files have records that duplicate the values in some
 * columns. These files can be viewed as tabular forms of tree structures.
 * Importing them into a tree can be achieved by identifying the columns that
 * represent parents. This class allows you to import a CSV into a tree by
 * specifying the hierarchy as a list of column headers, each of which
 * represents the parent of the next. For example, consider a file like this:
 * 
 * <pre>
 * A,B,C
 * 1,1,1
 * 1,1,2
 * 1,2,1
 * </pre>
 * 
 * We can see that column <code>A</code> always has value 1, while column
 * <code>B</code> has two values. This is a tree that looks like the following:
 * </ul> <li>A=1
 * <ul>
 * <li>B=1
 * <ul>
 * <li>C=1</li>
 * <li>C=2</li>
 * </ul>
 * </li>
 * <li>B=2
 * <ul>
 * <li>C=1</li>
 * </ul>
 * </li>
 * </ul>
 * </li> </ul> In Neo4j we would match this with
 * <code>MATCH (a)-[:child]->(b)-[:child]->(c)</code>
 * 
 * For the full specification of what is supported in loading the CSV and making
 * a tree, see the documentation for the CSVTreeBuilder class.
 * 
 * @author craig
 * 
 */
@Path("/service")
public class CSVTreeLoaderService {

	public static final Logger logger = Logger.getLogger(CSVTreeLoaderService.class);
	
	public static boolean verbose = false;

	/**
	 * This service will import a specified CSV file and convert it to a tree
	 * structure. For the full specification see the class documentation and the
	 * CSVTreeBuilder documentation.
	 * 
	 * @see org.amanzi.neo4j.csvtreeloader.CSVTreeLoaderService
	 * @see org.amanzi.neo4j.csvtreeloader.CSVTreeBuilder
	 * @param path
	 *            To the CSV file to be imported (use 'path=xyz')
	 * @param headers
	 *            Specification of multiple levels to the tree (use
	 *            'header=X.y.Z')
	 * @param leafProperties
	 *            Specification of multiple columns to add to leaf (use
	 *            leafProperty=A)
	 * @param leafPropertiesColumn
	 *            Specification of single column with JSON format containing
	 *            multiple properties to add to leaf (use leafProperties=B)
	 * @param db
	 *            GraphDatabaseService to use to access the database
	 *            (automatically added when run within Neo4j Server as
	 *            un-managed extension)
	 * @return Response containing a count of number of records successfully
	 *         added to the tree
	 */
	@GET
	@Path("/loadcsvtree")
	@Produces("application/json")
	public Response importCSV(@QueryParam("path") String path,
			@QueryParam(value = "header") List<String> headers,
			@QueryParam(value = "leafProperty") List<String> leafProperties,
			@QueryParam(value = "leafProperties") String leafPropertiesColumn,
			@QueryParam(value = "skip") Long skip,
			@QueryParam(value = "limit") Long limit,
			@QueryParam(value = "debug") Boolean debug,
			@Context GraphDatabaseService db) {
		try {
			logger.info("Processing 'loadcsvtree' request: path=%s", path);
			if (debug == null) debug = false;
			CSVTreeBuilder builder = new CSVTreeBuilder(path, headers, leafProperties, leafPropertiesColumn, db);
			builder.setPage(skip, limit);
			if (debug) builder.setLogger(System.out);
			HashMap<String, Object> response = new HashMap<String, Object>();
			response.put("count", builder.read());
			if (debug) builder.dumpTrees(10);
			return Response.ok().entity(new ObjectMapper().writeValueAsString(response)).build();
		} catch (IOException e) {
			logger.error("Error processing 'loadcsvtree' request: path=%s: %s", path, e.getMessage());
			return Response.status(404).entity(e.getMessage()).build();
		}
	}

}
