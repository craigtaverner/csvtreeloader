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
		importFromCSV(restRequest, "non-existant.csv", columnHeaders, leafProperties, leafPropertiesColumn, 404, 0);
		importFromCSV(restRequest, "samples/353333333333333.csv", columnHeaders, leafProperties, leafPropertiesColumn, 200, 122);
		//importFromCSV(restRequest, "samples/load_config_access.csv", columnHeaders, leafProperties, leafPropertiesColumn, 200, 1000);
		server.stop();
	}

	private void importFromCSV(RestRequest restRequest, String path, String[] columnHeaders, String[] leafProperties,
			String leafPropertiesColumn, int rc, int records) throws JsonProcessingException, IOException {
		System.out.println("Running CSV Import from: " + path);
		String query = "service/loadcsvtree?skip=0&limit=1000&path=" + path;
		if (columnHeaders != null) {
			query += "&" + ACollections.join(columnHeaders, "&header=");
		}
		if (leafProperties != null) {
			query += "&" + ACollections.join(leafProperties, "&leafProperty=");
		}
		if (leafPropertiesColumn != null) {
			query += "&leafProperties=" + leafPropertiesColumn;
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
