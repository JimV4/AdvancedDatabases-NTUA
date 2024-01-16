#!/bin/bash

# Stop Hadoop DFS
echo "Stopping Hadoop DFS..."
stop-dfs.sh

# Start Hadoop DFS
echo "Starting Hadoop DFS..."
start-dfs.sh

# Stop YARN
echo "Stopping YARN..."
stop-yarn.sh

# Start YARN
echo "Starting YARN..."
start-yarn.sh

# Stop Spark History Server
echo "Stopping Spark History Server..."
$SPARK_HOME/sbin/stop-history-server.sh

# Start Spark History Server
echo "Starting Spark History Server..."
$SPARK_HOME/sbin/start-history-server.sh

echo "Commands executed successfully."
