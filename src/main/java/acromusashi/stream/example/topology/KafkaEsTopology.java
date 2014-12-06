/**
* Copyright (c) Acroquest Technology Co, Ltd. All Rights Reserved.
* Please read the associated COPYRIGHTS file for more details.
*
* THE SOFTWARE IS PROVIDED BY Acroquest Technolog Co., Ltd.,
* WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
* BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
* IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDER BE LIABLE FOR ANY
* CLAIM, DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING
* OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
*/
package acromusashi.stream.example.topology;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import acromusashi.stream.component.elasticsearch.bolt.ElasticSearchBolt;
import acromusashi.stream.component.elasticsearch.bolt.EsTupleConverter;
import acromusashi.stream.component.elasticsearch.bolt.JsonConverter;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.topology.BaseTopology;
import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;

/**
 * KafkaからJSON文字列を取得し、ElasticSearchに投入するTopology
 *
 * @author kimura
 */
public class KafkaEsTopology extends BaseTopology
{
    /**
     * コンストラクタ
     *
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public KafkaEsTopology(String topologyName, Config config)
    {
        super(topologyName, config);
    }

    /**
     * プログラムエントリポイント<br/>
     * <ul>
     * <li>起動引数:arg[0] 設定値を記述したyamlファイルパス</li>
     * <li>起動引数:arg[1] Stormの起動モード(true:LocalMode、false:DistributeMode)</li>
     * </ul>
     * @param args 起動引数
     * @throws Exception 初期化例外発生時
     */
    public static void main(String[] args) throws Exception
    {
        // プログラム引数の不足をチェック
        if (args.length < 2)
        {
            System.out.println("Usage: java " + KafkaEsTopology.class.getName()
                    + " ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        BaseTopology topology = new KafkaEsTopology("KafkaEsTopology", conf);
        topology.buildTopology();
        topology.submitTopology(isLocal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void buildTopology() throws Exception
    {
        // @formatter:off
        // Get setting from StormConfig Object
        // Get KafkaSpout Config
        int kafkaPara = StormConfigUtil.getIntValue(getConfig(), "kafka.spout.parallelism", 1);
        String kafkaTopic = StormConfigUtil.getStringValue(getConfig(), "kafka.topic", "");
        String kafkaZkServerStr = StormConfigUtil.getStringValue(getConfig(), "kafka.zookeeper.server.str", "localhost:2181");
        String kafkaZkRoot = StormConfigUtil.getStringValue(getConfig(), "kafka.zookeeper.root", "");
        String kafkaConsumerId = StormConfigUtil.getStringValue(getConfig(), "kafka.consumer.id", "KafkaEsTopology");
        // Get ElasticSearchBolt Config
        int esBoltPara = StormConfigUtil.getIntValue(getConfig(), "elasticsearch.bolt.parallelism", 1);
        String esClusterName = StormConfigUtil.getStringValue(getConfig(), "elasticsearch.cluster.name", "");
        String esServerStr = StormConfigUtil.getStringValue(getConfig(), "elasticsearch.server.str", "");
        String esIndex = StormConfigUtil.getStringValue(getConfig(), "elasticsearch.index", "");
        String esType = StormConfigUtil.getStringValue(getConfig(), "elasticsearch.type", "");
        String esField = StormConfigUtil.getStringValue(getConfig(), "elasticsearch.field", "");
        // @formatter:on

        // Topology Setting
        // Add Spout(KafkaSpout)
        ZkHosts zkHosts = new ZkHosts(kafkaZkServerStr, kafkaZkRoot);
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, kafkaTopic, kafkaZkRoot, kafkaConsumerId);
        spoutConfig.zkRoot = kafkaZkRoot;
        SchemeAsMultiScheme multiScheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.scheme = multiScheme;

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        getBuilder().setSpout("KafkaSpout", kafkaSpout, kafkaPara);

        // Add Bolt(KafkaSpout -> ElasticSearchBolt)
        EsTupleConverter converter = new JsonConverter(esIndex, esType, esField);
        ElasticSearchBolt elasticSearchBolt = new ElasticSearchBolt(converter);
        elasticSearchBolt.setClusterName(esClusterName);
        elasticSearchBolt.setServers(esServerStr);
        getBuilder().setBolt("ElasticSearchBolt", elasticSearchBolt, esBoltPara).shuffleGrouping(
                "KafkaSpout");
    }
}
