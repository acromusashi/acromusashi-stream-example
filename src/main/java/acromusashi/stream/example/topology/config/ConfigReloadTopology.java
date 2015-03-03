package acromusashi.stream.example.topology.config;

import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.topology.BaseTopology;
import backtype.storm.Config;

/**
 * 
 * @author kimura
 *
 */
public class ConfigReloadTopology extends BaseTopology
{

    /**
     * コンストラクタ
     *
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public ConfigReloadTopology(String topologyName, Config config)
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
            System.out.println("Usage: java " + ConfigReloadTopology.class.getName()
                    + " ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        BaseTopology topology = new ConfigReloadTopology("ConfigReloadTopology", conf);
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
        int spoutPara = StormConfigUtil.getIntValue(getConfig(), "ConfigReloadSpout.Parallelism", 1);
        int boltPara = StormConfigUtil.getIntValue(getConfig(), "ConfigReloadBolt.Parallelism", 1);
        
        boolean spoutReload = Boolean.parseBoolean(getConfig().get("ConfigReloadSpout.reload").toString());
        int spoutReloadInterval = StormConfigUtil.getIntValue(getConfig(), "ConfigReloadSpout.interval", 30);
        boolean boltReload = Boolean.parseBoolean(getConfig().get("ConfigReloadBolt.reload").toString());
        int boltReloadInterval = StormConfigUtil.getIntValue(getConfig(), "ConfigReloadBolt.interval", 30);
        
        ConfigReloadSpout spout = new ConfigReloadSpout();
        spout.setReloadConfig(spoutReload);
        spout.setReloadConfigIntervalSec(spoutReloadInterval);
        
        ConfigReloadBolt bolt = new ConfigReloadBolt();
        bolt.setReloadConfig(boltReload);
        bolt.setReloadConfigIntervalSec(boltReloadInterval);
        
        getBuilder().setSpout("ConfigReloadSpout", spout, spoutPara);
        getBuilder().setBolt("ConfigReloadBolt", bolt, boltPara).shuffleGrouping("ConfigReloadSpout");
    }
}
