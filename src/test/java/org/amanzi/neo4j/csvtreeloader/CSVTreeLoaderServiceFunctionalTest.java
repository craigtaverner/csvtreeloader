package org.amanzi.neo4j.csvtreeloader;

import com.sun.jersey.api.client.Client;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CSVTreeLoaderServiceFunctionalTest {

	public static final Client CLIENT = Client.create();
	public static final String MOUNT_POINT = "/ext";
	private ObjectMapper objectMapper = new ObjectMapper();

	@Test
	public void shouldGetValidInfo() throws IOException {
		NeoServer server = CommunityServerBuilder.server().onPort(7577)
				.withThirdPartyJaxRsPackage("org.amanzi.neo4j.csvtreeloader", MOUNT_POINT).build();
		server.start();

		RestRequest restRequest = new RestRequest(server.baseUri().resolve(MOUNT_POINT), CLIENT);
		String query = "service/csvtree";
		JaxRsResponse response = restRequest.get(query);
		assertEquals(200, response.getStatus());
		System.out.println("Got Import response: " + response.getEntity());
		JsonNode tree = objectMapper.readTree(response.getEntity().toString());
		String version = tree.get("version").asText();
		assertTrue("Version should be valid string", version.length() >= 5);
		
		server.stop();
	}
	
	@Test
	public void shouldImportFromCSV() throws IOException {
		NeoServer server = CommunityServerBuilder.server().onPort(7577)
				.withThirdPartyJaxRsPackage("org.amanzi.neo4j.csvtreeloader", MOUNT_POINT).build();
		server.start();

		String[] columnHeaders = new String[] { "DeviceID", "Day..EventDay", "Date.time.Event" };
		String[] leafProperties = new String[] { "Date", "Path", "UTC" };
		String leafPropertiesColumn = "Params";
		RestRequest restRequest = new RestRequest(server.baseUri().resolve(MOUNT_POINT), CLIENT);
		importFromCSV(restRequest, "non-existant.csv", columnHeaders, leafProperties, leafPropertiesColumn, null, 404, 0, true);
		importFromCSV(restRequest, "samples/353333333333333.csv", columnHeaders, leafProperties, leafPropertiesColumn, null, 200, 122, false);
		//importFromCSV(restRequest, "samples/load_config_access.csv", columnHeaders, leafProperties, leafPropertiesColumn, 200, 1000, false);
		server.stop();
	}

	@Test
	public void shouldImportFromConfigCSV() throws IOException {
		NeoServer server = CommunityServerBuilder.server().onPort(7577)
				.withThirdPartyJaxRsPackage("org.amanzi.neo4j.csvtreeloader", MOUNT_POINT).build();
		server.start();
		
		String[] columnHeaders = new String[] { "Device.deviceid",
				"Version.version_name.GeoptimaVersion.Version%20Props", "Day..EventDay",
				"Date.time.ConfigCheck" };
		String[] leafProperties = new String[] { "Date.time", "UTC" };
		String leafPropertiesColumn = "Params";
		String treeSpec = "Device-versions->GeoptimaVersion-days->EventDay-checks->ConfigCheck";
		RestRequest restRequest = new RestRequest(server.baseUri().resolve(MOUNT_POINT), CLIENT);
		importFromCSV(restRequest, "samples/short_config.csv", columnHeaders, leafProperties, leafPropertiesColumn, treeSpec, 200, 5, true);
		server.stop();
	}

	@Test
	public void shouldManageInvalidRequests() throws IOException {
		NeoServer server = CommunityServerBuilder.server().onPort(7577)
				.withThirdPartyJaxRsPackage("org.amanzi.neo4j.csvtreeloader", MOUNT_POINT).build();
		server.start();

		RestRequest restRequest = new RestRequest(server.baseUri().resolve(MOUNT_POINT), CLIENT);
		importFromCSV(restRequest, null, null, null, null, null, 404, 0, false);
		importFromCSV(restRequest, "samples/353333333333333.csv", null, null, null, null, 200, 122, false);
		server.stop();		
	}

	private void importFromCSV(RestRequest restRequest, String path, String[] columnHeaders, String[] leafProperties,
			String leafPropertiesColumn, String treeSpec, int rc, int records, boolean debug) throws JsonProcessingException, IOException {
		System.out.println("Running CSV Import from: " + path);
		String query = "service/loadcsvtree?skip=0&limit=1000";
		if (path != null) {
			query += "&path=" + path;
		}
		if (columnHeaders != null) {
			query += "&header=" + ACollections.join(columnHeaders, "&header=");
		}
		if (leafProperties != null) {
			query += "&leafProperty=" + ACollections.join(leafProperties, "&leafProperty=");
		}
		if (leafPropertiesColumn != null) {
			query += "&leafProperties=" + leafPropertiesColumn;
		}
		if (treeSpec != null) {
			query += "&tree=" + java.net.URLEncoder.encode(treeSpec, "US-ASCII");
		}
		if (debug) {
			query += "&debug=true";
		}
		JaxRsResponse response = restRequest.get(query);
		assertEquals(rc, response.getStatus());
		if (rc == 200) {
			System.out.println("Got Import response: " + response.getEntity());
			JsonNode tree = objectMapper.readTree(response.getEntity().toString());
			int count = tree.get("count").asInt();
			assertEquals(records, count);
		}
	}
}
