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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import acromusashi.stream.bolt.hbase.CamelHbaseStoreBolt;
import acromusashi.stream.bolt.hbase.CellDefine;
import acromusashi.stream.component.snmp.converter.SnmpConverter;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.entity.Message;
import acromusashi.stream.example.spout.PeriodicalMessageGenSpout;
import acromusashi.stream.topology.BaseTopology;
import backtype.storm.Config;

/**
 * HBase DataStore用のTopologyを起動する。
 * <br/>
 * Topologyの動作フローは下記の通り。<br/>
 * <ol>
 * <li>PeriodicalMessageGenSpoutにてメッセージを生成する</li>
 * <li>CamelHbaseStoreBoltにて[hbase-site.xml]に指定したHBaseに対してデータを投入する</li>
 * </ol>
 * 
 * yamlファイルから読み込む設定値
 * <ul>
 * <li>MessageGenSpout.Parallelism : PeriodicalMessageGenSpoutの並列度(デフォルト値:1)</li>
 * <li>CamelHBaseBolt.Parallelism : CamelHbaseStoreBoltの並列度(デフォルト値:1)</li>
 * <li>CamelContext.Path : CamelHBaseBoltにおいて起動するCamelコンテキストパス(デフォルト値:file:/opt/storm/conf/camel-context-example-hbase.xml)</li>
 * <li>HBaseSchema.Define : CamelHBaseBoltにて投入するHBaseスキーマ定義。【Family】_【Quantifier】形式(デフォルト値:無)</li>
 * </ul>
 * @author otoda
 */
public class HbaseStoreTopology extends BaseTopology
{
    /**
     * コンストラクタ
     * 
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public HbaseStoreTopology(String topologyName, Config config)
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
            System.out.println("Usage: java HbaseStoreTopology ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        BaseTopology topology = new HbaseStoreTopology("HbaseStoreTopology", conf);
        topology.buildTopology();
        topology.submitTopology(isLocal);
    }

    @Override
    public void buildTopology() throws Exception
    {
        // Get setting from StormConfig Object
        int msgGenSpoutPara = StormConfigUtil.getIntValue(getConfig(),
                "MessageGenSpout.Parallelism", 1);
        int hbaseBoltPara = StormConfigUtil.getIntValue(getConfig(), "CamelHBaseBolt.Parallelism",
                1);
        String contextUri = StormConfigUtil.getStringValue(getConfig(), "CamelContext.Path",
                "file:/opt/storm/conf/camel-context-example-hbase.xml");
        List<String> cellDefineList = StormConfigUtil.getStringListValue(getConfig(),
                "HBaseSchema.Define");

        // Topology Setting
        // Add Spout(PeriodicalMessageGenSpout)
        PeriodicalMessageGenSpout messageGenSpout = new PeriodicalMessageGenSpout();
        getBuilder().setSpout("MessageGenSpout", messageGenSpout, msgGenSpoutPara);

        // Add Bolt(PeriodicalMessageGenSpout -> CamelHbaseStoreBolt)
        CamelHbaseStoreBolt hbaseStoreBolt = new CamelHbaseStoreBolt();
        hbaseStoreBolt.setConverter(new SnmpConverter());
        hbaseStoreBolt.setApplicationContextUri(contextUri);
        getBuilder().setBolt("CamelHBaseBolt", hbaseStoreBolt, hbaseBoltPara).localOrShuffleGrouping(
                "MessageGenSpout");

        List<CellDefine> cellList = new ArrayList<CellDefine>();

        for (String targetCell : cellDefineList)
        {
            String[] cell = StringUtils.split(targetCell, "_");
            cellList.add(new CellDefine(cell[0], cell[1]));
        }

        hbaseStoreBolt.setCellDefineList(cellList);

        // Regist Serialize Setting.
        getConfig().registerSerialization(Message.class);
    }
}
