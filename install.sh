#!/bin/sh

VERSION=`grep version pom.xml | head -1 | sed -e 's/[\<\>]/ /g' | awk '{print $2}'`

if [ -f target/csvtreeloader-$VERSION.jar ] ; then
  echo "Installing target/csvtreeloader-$VERSION.jar"
else
  mvn clean install
fi

sudo rm -f /var/lib/neo4j/plugins/csvtreeloader-*-SNAPSHOT.jar
sudo cp target/csvtreeloader-*-SNAPSHOT.jar /var/lib/neo4j/plugins/

ls -ltr /var/lib/neo4j/plugins/

sudo /etc/init.d/neo4j-service restart
