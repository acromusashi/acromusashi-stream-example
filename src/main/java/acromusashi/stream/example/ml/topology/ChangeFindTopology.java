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
package acromusashi.stream.example.ml.topology;

import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.impl.CombinerAggregatorCombineImpl;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.example.ml.trident.ResultPrintFunction;
import acromusashi.stream.ml.loganalyze.ApacheLog;
import acromusashi.stream.ml.loganalyze.ApacheLogAggregator;
import acromusashi.stream.ml.loganalyze.ApacheLogSplitFunction;
import acromusashi.stream.ml.loganalyze.ChangeFindFunction;
import acromusashi.stream.topology.BaseTridentTopology;
import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

/**
 * Apacheのログ中のレスポンスタイムに対してChangeFindアルゴリズムを適用するTridentTopology<br>
 * KafkaからApacheログ(JSON形式)を読み込み、Apacheホスト毎にグルーピングをかけてレスポンスタイムに対するChangeFinderアルゴリズムによる変化点検出を行う。<br>
 * 変化点検出後、ホスト毎に統計を行い、結果をログに出力する。<br>
 * <br>
 * Topologyの動作フローは下記の通り。<br>
 * <ol>
 * <li>TridentKafkaSpoutにてKafkaからApacheログ(JSON形式)を読み込む。</li>
 * <li>ApacheLogSplitFunctionにてJSONをエンティティに変換する。</li>
 * <li>ChangeFindFunctionにてレスポンスタイムに対するChangeFinderアルゴリズムによる変化点検出を行い、各。</li>
 * <li>ApacheLogAggregatorにてホスト毎に統計を行う。</li>
 * <li>ChangeFindFunctionにて統計結果をログ出力する。</li>
 * </ol>
 * 
 * yamlファイルから読み込む設定値については「EndoSnipeTridentTopology.yaml」参照。
 * 
 * @author kimura
 */
public class ChangeFindTopology extends BaseTridentTopology
{
    /**
     * Topology名称、Storm設定を指定してインスタンスを生成する。
     * 
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public ChangeFindTopology(String topologyName, Config config)
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
            System.out.println("Usage: java " + ChangeFindTopology.class.getName()
                    + " ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        ChangeFindTopology dcTridentTopology = new ChangeFindTopology("EndoSnipeTridentTopology",
                conf);
        StormTopology topology = dcTridentTopology.buildTridentTopology();
        dcTridentTopology.submitTopology(topology, isLocal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StormTopology buildTridentTopology()
    {
        // @formatter:off
        int parallelism = StormConfigUtil.getIntValue(this.config, "topology.parallelismHint", 1);
        // TridentKafkaSpout用設定値読み込み
        String kafkaTopic = StormConfigUtil.getStringValue(this.config, "kafka.topic", "");
        String kafkaZkServerStr = StormConfigUtil.getStringValue(this.config, "kafka.zookeeper.server.str", "localhost:2181");
        String kafkaZkRoot = StormConfigUtil.getStringValue(this.config, "kafka.zookeeper.root", "");
        String kafkaConsumerId = StormConfigUtil.getStringValue(this.config, "kafka.consumer.id", "EndoSnipeTridentTopology");
        // ApacheLogSplitFunction用設定値読み込み
        String dateFormatStr = StormConfigUtil.getStringValue(this.config, "apache.date.format", "yyyy-MM-dd'T'HH:mm:SSSZ");
        // ChangeFindFunction用設定値読み込み
        int arDimension = StormConfigUtil.getIntValue(this.config, "changefinder.ardimension", 2);
        double forgetability = Double.valueOf(StormConfigUtil.getStringValue(this.config, "changefinder.forgetability", "0.05"));
        int smoothingWindow = StormConfigUtil.getIntValue(this.config, "changefinder.smoothingwindow", 2);
        double scoreThreshold = Double.valueOf(StormConfigUtil.getStringValue(this.config, "changefinder.threshold", "15.0"));
        // @formatter:on

        // TridentKafkaSpoutを初期化
        ZkHosts zkHosts = new ZkHosts(kafkaZkServerStr, kafkaZkRoot);
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zkHosts, kafkaTopic,
                kafkaConsumerId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout tridentKafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);

        // ApacheLogSplitFunctionを初期化
        ApacheLogSplitFunction apacheLogSplitFunction = new ApacheLogSplitFunction();
        apacheLogSplitFunction.setDateFormatStr(dateFormatStr);

        // ChangeFindFunctionを初期化
        ChangeFindFunction cfFunction = new ChangeFindFunction();
        cfFunction.setArDimensionNum(arDimension);
        cfFunction.setForgetability(forgetability);
        cfFunction.setSmoothingWindow(smoothingWindow);
        cfFunction.setScoreThreshold(scoreThreshold);

        TridentTopology topology = new TridentTopology();

        // 以下の順でTridentTopologyにSpout/Functionを登録する。
        // 1.TridentKafkaSpout:KafkaからApacheログ(JSON形式)を取得
        //
        // ※全体並列度を「topology.parallelismHint」の設定値に併せて設定
        // 
        // >(each:各メッセージに対して実行)
        // 2.ApacheLogSplitFunction:受信したApacheログ(JSON形式)をエンティティに変換し、送信
        //
        // ※エンティティを「IPaddress」の値毎にグルーピング
        //
        // >(each:各メッセージに対して実行)
        // 3.ChangeFindFunction:受信したApacheログのレスポンスタイムを用いて変化点スコアを算出
        // 
        // >(partitionAggregate:グルーピングしたエンティティを統合)
        // 4.ApacheLogAggregator:受信したApacheログの統計値を算出
        //
        // >(each:各メッセージに対して実行)
        // 5.ResultPrintFunction:受信した統計値をログ出力
        // @formatter:off
        topology.newStream("TridentKafkaSpout", tridentKafkaSpout).parallelismHint(parallelism)
            .each(new Fields("str"), apacheLogSplitFunction, new Fields("IPaddress", "responseTime"))
            .groupBy(new Fields("IPaddress"))
            .each(new Fields("IPaddress", "responseTime"), cfFunction, new Fields("webResponse"))
            .partitionAggregate(new Fields("webResponse"), new CombinerAggregatorCombineImpl(new ApacheLogAggregator()), new Fields("average"))
            .each(new Fields("average"), new ResultPrintFunction(), new Fields("count"));
        // @formatter:on

        // Topology内でTupleに設定するエンティティをシリアライズ登録
        this.config.registerSerialization(ApacheLog.class);

        return topology.build();
    }
}
