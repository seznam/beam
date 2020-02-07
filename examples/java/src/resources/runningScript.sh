#!/bin/bash

set -ex
FLINK_HOME=/www/flink/current/
export HADOOP_CLASSPATH="$(hadoop classpath)"
export HADOOP_CONF_DIR="/etc/hadoop/conf"

exec $FLINK_HOME/bin/flink run -m yarn-cluster -yn 2 --class org.apache.beam.examples.CheckpointFailingExample /home/myuser/beam-examples-java-2.17.0-SNAPSHOT.jar \
--runner=FlinkRunner --checkpointingInterval=60000 --objectReuse=true --minPauseBetweenCheckpoints=10000 --inputFilePath=hdfs:///tmp/myFolder/part-r-*