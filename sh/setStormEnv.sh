#!/bin/bash
#-----------------------------------------------------------------------------
# CamelConsoleの環境設定ファイル
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------

# Classパス修正
STREAM_CLASSPATH=\
${STREAM_HOME}/lib/*:\
${STREAM_HOME}/lib/camel/*:\
${STREAM_HOME}/lib/hbase/*:\
${STREAM_HOME}/lib/hdfs/*:\
${STREAM_HOME}/lib/jdbc/*:\
${STREAM_HOME}/lib/jetty/*:\
${STREAM_HOME}/lib/snmp/*:\
${STREAM_HOME}/lib/spring/*:\
${STREAM_HOME}/lib/storm/*:\
${STREAM_HOME}/lib/xmljson/*:\
${STREAM_HOME}/lib/conf
export STREAM_CLASSPATH
