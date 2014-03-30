#!/bin/sh

mvn clean install

sudo cp target/csvtreeloader-*-SNAPSHOT.jar /var/lib/neo4j/plugins/

ls -ltr /var/lib/neo4j/plugins/

sudo /etc/init.d/neo4j-service restart
