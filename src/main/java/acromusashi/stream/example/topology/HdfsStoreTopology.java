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

import acromusashi.stream.bolt.hdfs.HdfsStoreBolt;
import acromusashi.stream.component.snmp.converter.SnmpConverter;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.entity.Message;
import acromusashi.stream.example.spout.PeriodicalMessageGenSpout;
import acromusashi.stream.topology.BaseTopology;
import backtype.storm.Config;

/**
 * HDFS DataStore用のTopologyを起動する。
 * <br/>
 * Topologyの動作フローは下記の通り。<br/>
 * <ol>
 * <li>PeriodicalMessageGenSpoutにてメッセージを生成する</li>
 * <li>HdfsStoreBoltにて設定項目[hdfsstorebolt.～]に設定したHDFSに対してデータを投入する</li>
 * </ol>
 * 
 * <ul>
 * <li>MessageGenSpout.Parallelism : PeriodicalMessageGenSpoutの並列度(デフォルト値:1)</li>
 * <li>HdfsStoreBolt.Parallelism : HdfsStoreBoltの並列度(デフォルト値:1)</li>
 * <li>hdfsstorebolt.outputuri : HdfsStoreBoltにおける出力先URI(デフォルト値:無)</li>
 * <li>hdfsstorebolt.filenameheader : HDFSに出力するファイル名ヘッダ(デフォルト値:無)</li>
 * <li>hdfsstorebolt.interval : HDFSに出力する際のファイル切替インターバル(単位:分)(デフォルト値:10)</li>
 * <li>hdfsstorebolt.executepreprocess : 起動時HDFS上の.tmpファイルをリネームするか。Booleanオブジェクトを設定すること(デフォルト値:true)</li>
 * </ul>
 * @author otoda
 */
public class HdfsStoreTopology extends BaseTopology
{
    /**
     * コンストラクタ
     * 
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public HdfsStoreTopology(String topologyName, Config config)
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
            System.out.println("Usage: java HdfsStoreTopology ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        BaseTopology topology = new HdfsStoreTopology("HdfsStoreTopology", conf);
        topology.buildTopology();
        topology.submitTopology(isLocal);
    }

    @Override
    public void buildTopology() throws Exception
    {
        // Get setting from StormConfig Object
        int msgGenSpoutPara = StormConfigUtil.getIntValue(getConfig(),
                "MessageGenSpout.Parallelism", 1);
        int hdfsBoltPara = StormConfigUtil.getIntValue(getConfig(), "HdfsStoreBolt.Parallelism", 1);

        // Topology Setting
        // Add Spout(PeriodicalMessageGenSpout)
        PeriodicalMessageGenSpout spout = new PeriodicalMessageGenSpout();
        getBuilder().setSpout("MessageGenSpout", spout, msgGenSpoutPara);

        // Add Bolt(PeriodicalMessageGenSpout -> HdfsStoreBolt)
        HdfsStoreBolt bolt = new HdfsStoreBolt();
        bolt.setConverter(new SnmpConverter());
        getBuilder().setBolt("HdfsStoreBolt", bolt, hdfsBoltPara).shuffleGrouping("MessageGenSpout");

        // Regist Serialize Setting.
        getConfig().registerSerialization(Message.class);
    }
}
