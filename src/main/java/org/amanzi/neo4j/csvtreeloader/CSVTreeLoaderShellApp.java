package org.amanzi.neo4j.csvtreeloader;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

public class CSVTreeLoaderShellApp extends AbstractApp {
	public static final int DEFAULT_BATCH_SIZE = 1000;
	private boolean debug = false;

	private ExecutionEngine engine;

	{
		addOptionDefinition("i", new OptionDefinition(OptionValueType.MUST, "Input CSV/TSV file"));
		addOptionDefinition("b", new OptionDefinition(OptionValueType.MUST, "Batch Size default " + DEFAULT_BATCH_SIZE));
		addOptionDefinition("q", new OptionDefinition(OptionValueType.NONE, "Quoted Strings in file"));
	}

	protected ExecutionEngine getEngine() {
		if (engine == null)
			engine = new ExecutionEngine(getServer().getDb(), StringLogger.SYSTEM);
		return engine;
	}

	@Override
	public String getName() {
		return "import-tree";
	}

	@Override
	public GraphDatabaseShellServer getServer() {
		return (GraphDatabaseShellServer) super.getServer();
	}

	private static class ShellOutputStream extends OutputStream {

		private Output out;

		public ShellOutputStream(Output out) {
			this.out = out;
		}

		@Override
		public void write(int b) throws IOException {
			CharSequence c = "" + b;
			out.append(c);
		}
	}

	@Override
	public Continuation execute(AppCommandParser parser, Session session, final Output out) throws Exception {
		int batchSize = Integer.parseInt(parser.option("b", String.valueOf(DEFAULT_BATCH_SIZE)));
		long skip = Integer.parseInt(parser.option("s", "0"));
		long limit = Integer.parseInt(parser.option("l", "0"));
		boolean quotes = parser.options().containsKey("q");
		debug = parser.options().containsKey("d");
		String[] columnHeaders = parser.arguments().toArray(new String[0]);
		String leafPropertiesColumn = parser.option("c", null);
		ArrayList<String> leafProperties = new ArrayList<String>();
		leafProperties.add(parser.option("l", null));
		String inputFileName = parser.option("i", null);
		out.println(String.format("Infile %s quoted %s batch-size %d", name(inputFileName), quotes, batchSize));
		PrintStream shellOutput = new PrintStream(new ShellOutputStream(out));
		try {
			CSVTreeBuilder builder = new CSVTreeBuilder(inputFileName, columnHeaders, null, leafProperties,
					leafPropertiesColumn, this.getServer().getDb());
			builder.setPage(skip, limit);
			if (debug)
				builder.setLogger(shellOutput);
			HashMap<String, Object> response = new HashMap<String, Object>();
			response.put("count", builder.read());
			if (debug)
				builder.dumpAllTrees(10);
		} catch (IOException e) {
			out.println("Error importing " + inputFileName + ": " + e);
			e.printStackTrace(shellOutput);
		}
		return Continuation.INPUT_COMPLETE;
	}

	private String name(Object file) {
		if (file == null)
			return "(none)";
		return file.toString();
	}

}
