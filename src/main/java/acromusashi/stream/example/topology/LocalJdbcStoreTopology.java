package acromusashi.stream.example.topology;

import acromusashi.stream.bolt.jdbc.CamelJdbcStoreBolt;
import acromusashi.stream.component.snmp.converter.SnmpConverter;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.entity.Message;
import acromusashi.stream.example.spout.PeriodicalSnmpGenSpout;
import acromusashi.stream.topology.BaseTopology;
import backtype.storm.Config;

/**
 * StormTopologyから外部には接続せず、Snmp型共通メッセージをH2データベースに保存するTopologyを起動する。<br/>
 * <br/>
 * Topologyの動作フローは下記の通り。<br/>
 * <ol>
 * <li>PeriodicalSnmpGenSpoutにてSNMP形式の共通メッセージを生成する</li>
 * <li>CamelJdbcStoreBoltにて設定項目[dataSource]に設定したDataSourceに対してデータを投入する</li>
 * </ol>
 * 
 * yamlファイルから読み込む設定値
 * <ul>
 * <li>SnmpGenSpout.Parallelism : PeriodicalSnmpGenSpoutの並列度(デフォルト値:1)</li>
 * <li>JdbcStoreBolt.Parallelism : CamelJdbcStoreBoltの並列度(デフォルト値:1)</li>
 * <li>CamelContext.Path : JdbcStoreBoltにおいて起動するCamelコンテキストパス(デフォルト値:file:/opt/storm/conf/camel-context-example-jdbc.xml)</li>
 * </ul>
 * @author tsukano
 */
public class LocalJdbcStoreTopology extends BaseTopology
{
    /**
     * コンストラクタ
     * 
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public LocalJdbcStoreTopology(String topologyName, Config config)
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
            System.out.println("Usage: java LocalJdbcStoreTopology ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        BaseTopology topology = new LocalJdbcStoreTopology(
                "LocalJdbcStoreTopology", conf);
        topology.buildTopology();
        topology.submitTopology(isLocal);
    }

    @Override
    public void buildTopology() throws Exception
    {
        // Get setting from StormConfig Object
        int snmpGenSpoutPara = StormConfigUtil.getIntValue(getConfig(),
                "SnmpGenSpout.Parallelism", 1);
        int jdbcBoltPara = StormConfigUtil.getIntValue(getConfig(),
                "JdbcStoreBolt.Parallelism", 1);
        String contextUri = StormConfigUtil.getStringValue(getConfig(),
                "CamelContext.Path",
                "file:/opt/storm/conf/camel-context-example-jdbc.xml");

        // Topology Setting
        // Add Spout(PeriodicalSnmpGenSpout)
        PeriodicalSnmpGenSpout spout = new PeriodicalSnmpGenSpout();
        getBuilder().setSpout("SnmpGenSpout", spout, snmpGenSpoutPara);

        // Add Bolt(PeriodicalSnmpGenSpout -> CamelJdbcStoreBolt)
        CamelJdbcStoreBolt messageBolt = new CamelJdbcStoreBolt();
        messageBolt.setApplicationContextUri(contextUri);
        messageBolt.setConverter(new SnmpConverter());
        getBuilder().setBolt("JdbcStoreBolt", messageBolt, jdbcBoltPara).localOrShuffleGrouping(
                "SnmpGenSpout");

        // Regist Serialize Setting.
        getConfig().registerSerialization(Message.class);
    }
}
