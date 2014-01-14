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

import java.util.List;

import acromusashi.stream.bolt.MessageConvertBolt;
import acromusashi.stream.bolt.jdbc.CamelJdbcStoreBolt;
import acromusashi.stream.component.kestrel.spout.KestrelSpout;
import acromusashi.stream.component.snmp.converter.SnmpConverter;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.entity.Message;
import acromusashi.stream.topology.BaseTopology;
import backtype.storm.Config;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.SchemeAsMultiScheme;

/**
 * データベース用のTopologyを起動する。
 * <br/>
 * Topologyの動作フローは下記の通り。<br/>
 * <ol>
 * <li>KestrelSpoutにてSNMPメッセージをJSON形式で受信する</li>
 * <li>MessageConvertBoltにてJSON形式のTrapを共通メッセージ形式に変換する</li>
 * <li>CamelJdbcStoreBoltにて設定項目[dataSource]に設定したDataSourceに対してデータを投入する</li>
 * </ol>
 * 
 * yamlファイルから読み込む設定値
 * <ul>
 * <li>Kestrel.Hosts : Kestrelが配置されるホスト:Portの配列(デフォルト値:無)</li>
 * <li>Kestrel.QueueName : Kestrelのキュー名称(デフォルト値:MessageQueue)</li>
 * <li>KestrelSpout.Parallelism : KestrelSpoutの並列度(デフォルト値:1)</li>
 * <li>ConvertBolt.Parallelism : MessageConvertBoltの並列度(デフォルト値:1)</li>
 * <li>JdbcStoreBolt.Parallelism : CamelJdbcStoreBoltの並列度(デフォルト値:1)</li>
 * <li>CamelContext.Path : JdbcStoreBoltにおいて起動するCamelコンテキストパス(デフォルト値:file:/opt/storm/conf/camel-context-example-jdbc.xml)</li>
 * </ul>
 * @author kimura
 */
public class Snmp2JdbcStoreTopology extends BaseTopology
{
    /**
     * コンストラクタ
     * 
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public Snmp2JdbcStoreTopology(String topologyName, Config config)
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
            System.out.println("Usage: java Snmp2JdbcStoreTopology ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        BaseTopology topology = new Snmp2JdbcStoreTopology("Snmp2JdbcStoreTopology", conf);
        topology.buildTopology();
        topology.submitTopology(isLocal);
    }

    @Override
    public void buildTopology() throws Exception
    {
        // Get setting from StormConfig Object
        List<String> kestrelHosts = StormConfigUtil.getStringListValue(getConfig(), "Kestrel.Hosts");
        String kestrelQueueName = StormConfigUtil.getStringValue(getConfig(), "Kestrel.QueueName",
                "MessageQueue");
        int kestrelSpoutPara = StormConfigUtil.getIntValue(getConfig(), "KestrelSpout.Parallelism",
                1);
        int msgConvertPara = StormConfigUtil.getIntValue(getConfig(), "ConvertBolt.Parallelism", 1);
        int jdbcBoltPara = StormConfigUtil.getIntValue(getConfig(), "JdbcStoreBolt.Parallelism", 1);
        String contextUri = StormConfigUtil.getStringValue(getConfig(), "CamelContext.Path",
                "file:/opt/storm/conf/camel-context-example-jdbc.xml");

        // Topology Setting
        // Add Spout(KestrelSpout)
        KestrelSpout kestrelSpout = new KestrelSpout(kestrelHosts, kestrelQueueName,
                new SchemeAsMultiScheme(new StringScheme()));
        getBuilder().setSpout("KestrelSpout", kestrelSpout, kestrelSpoutPara);

        // Add Bolt(KestrelSpout -> MessageConvertBolt)
        MessageConvertBolt convertBolt = new MessageConvertBolt();
        convertBolt.setConverter(new SnmpConverter());
        getBuilder().setBolt("ConvertBolt", convertBolt, msgConvertPara).shuffleGrouping(
                "KestrelSpout");

        // Add Bolt(MessageConvertBolt -> CamelJdbcStoreBolt)
        CamelJdbcStoreBolt messageBolt = new CamelJdbcStoreBolt();
        messageBolt.setApplicationContextUri(contextUri);
        messageBolt.setConverter(new SnmpConverter());
        getBuilder().setBolt("JdbcBolt", messageBolt, jdbcBoltPara).localOrShuffleGrouping(
                "ConvertBolt");

        // Regist Serialize Setting.
        getConfig().registerSerialization(Message.class);
    }
}
