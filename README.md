Neo4j Unmanaged Extension for importing CSV files into Tree Structures
======================================================================

Many CSV files contain data that is better structured as a tree graph for
analysis.  For example, an apache event log might seem to be a collection of
disconnected events, but in reality there are attributes that connect them. 
For example, the browser IP, or the user accessing the service, or the day
or month of access, or the part of the web app being accessed, or the type
of REST call being made.  All these parameters could be structured as tree
structures.  This extension was written specifically in support of a project
that was using a Ruby script (and neography) to parse an apache logfile and
build statistics of the remote devices accessing the logs.  However,
standard neoclipse is too slow for parsing long files, and on a simple
laptop we got only 4 records per second imported.  This could have been
improved a lot with refactoring to the batch API, but even better with an
unmanaged extension.  Initial testing showed this extension to import over
1000 records per second on the same hardware.  This extension was written to
be more general than the needs of the original app, so it could be used to
build any depth tree from any CSV format data.

Installation
------------

This is an unmanaged extension. 

1. Build it: 

        mvn clean package

2. Copy target/csvtreeloader-1.0.jar to the plugins/ directory of your Neo4j server.

3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=org.amanzi.neo4j.csvtreeloader=/ext

4. Start Neo4j server.

5. Query it over HTTP:

        curl http://localhost:7474/ext/service/loadcsvtree?path=samples/353333333333333.csv&header=Device&header=Day..EventDay&header=Date.time.Event&leafProperty=UTC&leafProperty=Path&leafPropertiesColumn=Params

   Change the path field to the real location of the sample, reletive to the server.
