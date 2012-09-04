#!/bin/bash
#-----------------------------------------------------------------------------
# 外部には接続せず、共通メッセージをＤＢに蓄積するTopologyを起動
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# StreamExampleを配置した基準ディレクトリ

# スクリプトの配置ディレクトリを調べる
SCRIPT_DIR=`dirname ${0}`
STREAM_HOME=`cd ${SCRIPT_DIR}/../; pwd`

source ${STREAM_HOME}/bin/setStormEnv.sh

java                                     \
  -Dlog4j.configuration=log4j.properties \
  -cp ${STREAM_CLASSPATH}                \
  acromusashi.stream.example.topology.LocalJdbcStoreTopology \
  ${STREAM_HOME}/conf/LocalJdbcStoreTopology.yaml true

