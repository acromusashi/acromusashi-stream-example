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
import acromusashi.stream.component.kestrel.spout.KestrelSpout;
import acromusashi.stream.component.snmp.converter.SnmpConverter;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.entity.Message;
import acromusashi.stream.example.bolt.BlackHoleBolt;
import acromusashi.stream.topology.BaseTopology;
import backtype.storm.Config;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.SchemeAsMultiScheme;

/**
 * KestrelからSnmpメッセージを取得し、破棄するTopologyを起動する。
 * <br/>
 * Topologyの動作フローは下記の通り。<br/>
 * <ol>
 * <li>KestrelSpoutにてSNMPメッセージをJSON形式で受信する</li>
 * <li>MessageConvertBoltにてJSON形式のTrapを共通メッセージ形式に変換する</li>
 * <li>BlackHoleBoltにて共通メッセージを破棄する</li>
 * </ol>
 * 
 * yamlファイルから読み込む設定値
 * <ul>
 * <li>Kestrel.Hosts : Kestrelが配置されるホスト:Portの配列(デフォルト値:無)</li>
 * <li>Kestrel.QueueName : Kestrelのキュー名称(デフォルト値:MessageQueue)</li>
 * <li>KestrelSpout.Parallelism : KestrelSpoutの並列度(デフォルト値:1)</li>
 * <li>ConvertBolt.Parallelism : MessageConvertBoltの並列度(デフォルト値:1)</li>
 * <li>BlackHoleBolt.Parallelism : BlackHoleBoltの並列度(デフォルト値:1)</li>
 * </ul>
 * @author otoda
 */
public class Snmp2TrashTopology extends BaseTopology
{
    /**
     * コンストラクタ
     * 
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public Snmp2TrashTopology(String topologyName, Config config)
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
            System.out.println("Usage: java acromusashi.stream.example.topology.Snmp2TrashTopology ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        BaseTopology topology = new Snmp2TrashTopology("Snmp2TrashTopology", conf);
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
        int blackholeBoltPara = StormConfigUtil.getIntValue(getConfig(),
                "BlackHoleBolt.Parallelism", 1);

        // Topology Setting
        // Add Spout(KestrelSpout)
        KestrelSpout kestrelSpout = new KestrelSpout(kestrelHosts, kestrelQueueName,
                new SchemeAsMultiScheme(new StringScheme()));
        getBuilder().setSpout("KestrelSpout", kestrelSpout, kestrelSpoutPara);

        // Add Bolt(KestrelSpout -> MessageConvertBolt)
        MessageConvertBolt convertBolt = new MessageConvertBolt();
        convertBolt.setConverter(new SnmpConverter());
        getBuilder().setBolt("ConvertBolt", convertBolt, msgConvertPara).localOrShuffleGrouping(
                "KestrelSpout");

        // Add Bolt(MessageConvertBolt -> HdfsStoreBolt)
        BlackHoleBolt bolt = new BlackHoleBolt();
        getBuilder().setBolt("BlackHoleBolt", bolt, blackholeBoltPara).localOrShuffleGrouping(
                "ConvertBolt");

        // Regist Serialize Setting.
        getConfig().registerSerialization(Message.class);
    }
}
