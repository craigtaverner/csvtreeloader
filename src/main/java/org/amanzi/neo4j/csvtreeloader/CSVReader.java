package org.amanzi.neo4j.csvtreeloader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * The CSV reader can read a CSV file with either '\t' or ',' separated fields
 * (auto-detecting the type from the header line), and create Map<String,String>
 * of all records in the file, based on the keys in the header line.
 * 
 * @author craig
 * 
 */
class CSVReader {
	private BufferedReader in;
	private String[] headers;
	private HashMap<String, Integer> headerMap = null;
	private HashMap<String, String> record = new HashMap<String, String>();
	private String separator;

	public CSVReader(String path) throws IOException {
		this.in = new BufferedReader(new FileReader(path));
		String headerLine = this.in.readLine();
		this.separator = selectSeparator(headerLine);
		this.headers = headerLine.split(this.separator);
		this.headerMap = new HashMap<String, Integer>();
		for (String head : this.headers) {
			this.headerMap.put(head, this.headerMap.size());
		}
	}

	public static String selectSeparator(String line) {
		int bestLen = 0;
		String best = null;
		for (String separator : new String[] { "\t", "," }) {
			String[] fields = line.split(separator);
			if (fields.length >= bestLen) {
				bestLen = fields.length;
				best = separator;
			}
		}
		return best;
	}

	public HashMap<String, String> readRecord() throws IOException {
		String line = this.in.readLine();
		record.clear(); // reuse object for reduced gc
		if (line == null) {
			return null;
		} else {
			String[] fields = line.split(this.separator);
			HashMap<String, String> map = new HashMap<String, String>();
			for (String field : fields) {
				map.put(headers[map.size()], field);
			}
			fields = null;
			return map;
		}
	}
}
