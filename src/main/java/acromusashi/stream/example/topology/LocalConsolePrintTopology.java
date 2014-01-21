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

import acromusashi.stream.component.kestrel.spout.KestrelSpout;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.entity.Message;
import acromusashi.stream.example.bolt.ConsolePrintBolt;
import acromusashi.stream.topology.BaseTopology;
import backtype.storm.Config;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.SchemeAsMultiScheme;

/**
 * StormTopologyから外部には接続せず、共通メッセージをコンソール出力するTopologyを起動する。<br/>
 * Topologyの動作フローは下記の通り。<br/>
 * <ol>
 * <li>PeriodicalMessageGenSpoutにて共通メッセージを生成する</li>
 * <li>CamelHbaseStoreBoltにて共通メッセージをコンソールに出力する</li>
 * </ol>
 * 
 * yamlファイルから読み込む設定値
 * <ul>
 * <li>MessageGenSpout.Parallelism : PeriodicalMessageGenSpoutの並列度(デフォルト値:2)</li>
 * <li>ConsolePrintBolt.Parallelism : CamelHbaseStoreBoltの並列度(デフォルト値:2)</li>
 * </ul>
 * @author kimura
 */
public class LocalConsolePrintTopology extends BaseTopology
{
    /**
     * コンストラクタ
     * 
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public LocalConsolePrintTopology(String topologyName, Config config)
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
            System.out.println("Usage: java LocalConsolePrintTopology ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        BaseTopology topology = new LocalConsolePrintTopology("LocalConsolePrintTopology", conf);
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
        int consoleBoltPara = StormConfigUtil.getIntValue(getConfig(),
                "ConsolePrintBolt.Parallelism", 2);

        // Topology Setting
        // Add Spout(KestrelSpout)
        KestrelSpout kestrelSpout = new KestrelSpout(kestrelHosts, kestrelQueueName,
                new SchemeAsMultiScheme(new StringScheme()));
        getBuilder().setSpout("KestrelSpout", kestrelSpout, kestrelSpoutPara);

        // Add Bolt(KestrelSpout -> ConsolePrintBolt)
        ConsolePrintBolt printBolt = new ConsolePrintBolt();
        getBuilder().setBolt("ConsolePrintBolt", printBolt, consoleBoltPara).localOrShuffleGrouping(
                "KestrelSpout");

        // Regist Serialize Setting.
        getConfig().registerSerialization(Message.class);
    }
}
