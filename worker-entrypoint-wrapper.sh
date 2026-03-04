#!/bin/bash
export HADOOP_HOME=/opt/hadoop-2.7.4
echo "Setting CLASSPATH from Hadoop (expanded)..."
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath | tr ':' '\n' | while read entry; do
  if [[ "$entry" == *"*" ]]; then
    dir=${entry%/*}
    for jar in $dir/*.jar; do
      echo -n "$jar:"
    done
  else
    echo -n "$entry:"
  fi
done | sed 's/:$//')
if [ $? -ne 0 ] || [ -z "$CLASSPATH" ]; then
    echo "Failed to set CLASSPATH, exiting."
    exit 1
fi
echo "CLASSPATH set successfully (length: ${#CLASSPATH})"
# 调用原始 entrypoint 脚本
exec /opt/entrypoint.sh "$@"
