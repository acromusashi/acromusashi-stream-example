package acromusashi.stream.example.topology;

import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.entity.Message;
import acromusashi.stream.example.bolt.ConsolePrintBolt;
import acromusashi.stream.example.spout.PeriodicalMessageGenSpout;
import acromusashi.stream.topology.BaseTopology;
import backtype.storm.Config;

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
        BaseTopology topology = new LocalConsolePrintTopology(
                "LocalConsolePrintTopology", conf);
        topology.buildTopology();
        topology.submitTopology(isLocal);
    }

    @Override
    public void buildTopology() throws Exception
    {
        // Get setting from StormConfig Object
        int messageGenPara = StormConfigUtil.getIntValue(getConfig(),
                "MessageGenSpout.Parallelism", 2);
        int consoleBoltPara = StormConfigUtil.getIntValue(getConfig(),
                "ConsolePrintBolt.Parallelism", 2);

        // Topology Setting
        // Add Spout(PeriodicalMessageGenSpout)
        PeriodicalMessageGenSpout messageGenSpout = new PeriodicalMessageGenSpout();
        getBuilder().setSpout("MessageGenSpout", messageGenSpout,
                messageGenPara);

        // Add Bolt(PeriodicalMessageGenSpout -> ConsolePrintBolt)
        ConsolePrintBolt printBolt = new ConsolePrintBolt();
        getBuilder().setBolt("ConsolePrintBolt", printBolt, consoleBoltPara).localOrShuffleGrouping(
                "MessageGenSpout");

        // Regist Serialize Setting.
        getConfig().registerSerialization(Message.class);
    }
}
