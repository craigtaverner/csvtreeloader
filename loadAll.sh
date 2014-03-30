#!/bin/bash

SERVICE="http://localhost:7474/ext/service/loadcsvtree"
HEADERS="header=Device.deviceid..versions&header=Version.version_name.GeoptimaVersion.days.Version%20Props&header=Day..EventDay.checks&header=Date.time.GeoptimaEvent&leafProperty=Date.time"
LEAF="leafProperty=UTC&leafProperties=Params"
BASE="$(pwd)/csv"

sudo /etc/init.d/neo4j-service stop
sudo rm -Rf /var/lib/neo4j/data/graph.db/*
sudo /etc/init.d/neo4j-service start

for path in $BASE/apache*.csv
do
  echo "Importing $path"
  REQUEST="$SERVICE?path=$path&$HEADERS&$LEAF"
  echo "Requesting: $REQUEST"
  time (curl -i $REQUEST ; echo)
done
