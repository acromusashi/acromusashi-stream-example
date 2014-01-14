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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.ml.anomaly.lof.LofConfKey;
import acromusashi.stream.ml.anomaly.lof.LofModelPrinter;
import acromusashi.stream.ml.anomaly.lof.LofPointCreator;
import acromusashi.stream.ml.anomaly.lof.LofQuery;
import acromusashi.stream.ml.anomaly.lof.LofResultPrinter;
import acromusashi.stream.ml.anomaly.lof.LofUpdater;
import acromusashi.stream.ml.anomaly.lof.entity.LofPoint;
import acromusashi.stream.ml.anomaly.lof.state.InfinispanLofStateFactory;
import acromusashi.stream.ml.common.spout.TextReadBatchSpout;
import acromusashi.stream.topology.BaseTridentTopology;
import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * LOFスコア算出を行うTopology<br>
 * ファイルから点データを読み込み、LOFアルゴリズムを使用してスコア算出を行う。<br>
 * LOFスコア算出を行った結果の学習データをInfinispanに保存する。<br>
 * 設定時間間隔で学習モデルのマージを行い、複数スレッド間で発生した差分の補正を行う。<br>
 * DRPCで判定用データを受信し、クラスタリングを行った結果を返す。<br>
 * <br>
 * Topologyの動作フローは下記の通り。<br>
 * 入力Stream
 * <ol>
 * <li>TextReadBatchSpoutにてファイルを読み込み、1行を1データとして分割する。</li>
 * <li>LofCreatorにてLOF判定用データに変換する。</li>
 * <li>LofUpdaterにてLOF判定用データを学習モデルに反映し、学習モデルの更新を行う。</li>
 * </ol>
 * 判定Stream
 * <ol>
 * <li>DRPCSpoutにてDRPCクライアントから送信された判定用データを受け取る。</li>
 * <li>LofCreatorにてLOF判定用データに変換する。</li>
 * <li>LofQueryにて学習モデルを用いてLOFスコア算出を行い、結果を返す。</li>
 * </ol>
 * 
 * yamlファイルから読み込む設定値については「LofTopology.yaml」参照。
 * 
 * @author kimura
 */
public class LofTopology extends BaseTridentTopology
{
    /**
     * Topology名称、Storm設定を指定してインスタンスを生成する。
     * 
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public LofTopology(String topologyName, Config config)
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
            System.out.println("Usage: java " + LofTopology.class.getName()
                    + " ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        LofTopology lofTopology = new LofTopology("LofTopology", conf);
        StormTopology topology = lofTopology.buildTridentTopology();
        lofTopology.submitTopology(topology, isLocal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StormTopology buildTridentTopology()
    {
        // @formatter:off
        int parallelism = StormConfigUtil.getIntValue(this.config, "topology.parallelismHint", 1);
        // 学習データ読込Spout用設定値読み込み
        String dataFilePath = StormConfigUtil.getStringValue(this.config, "lof.datafilepath", null);
        String baseFileName = StormConfigUtil.getStringValue(this.config, "lof.datafilebasename", null);
        boolean isFileReload = Boolean.parseBoolean(StormConfigUtil.getStringValue(this.config, "lof.datafilereload", "true"));
        int maxBatchSize = StormConfigUtil.getIntValue(this.config, "lof.maxbatchsize", 0);
        // StateFactory用設定値読み込み
        String serverUri = StormConfigUtil.getStringValue(this.config, "lof.stateservers", null);
        String tableName = StormConfigUtil.getStringValue(this.config, "lof.cachename", null);
        String stateBaseName = StormConfigUtil.getStringValue(this.config, LofConfKey.STATE_BASE_NAME, null);
        int mergeInterval = StormConfigUtil.getIntValue(this.config, "lof.merge.interval.secs", 1);
        int lifespan = StormConfigUtil.getIntValue(this.config, "lof.lifespan.secs", 1);
        // Creator用設定値読み込み
        String delimeter = StormConfigUtil.getStringValue(this.config, "lof.delimeter", ",");
        // Updater用設定値読み込み
        boolean hasIntermediate = Boolean.parseBoolean(StormConfigUtil.getStringValue(this.config, LofConfKey.HAS_INTERMEDIATE, "true"));
        boolean alwaysUpdate = Boolean.parseBoolean(StormConfigUtil.getStringValue(this.config, LofConfKey.ALWAYS_UPDATE_MODEL, "false"));
        int kn = StormConfigUtil.getIntValue(this.config, LofConfKey.KN, 1);
        int minDataCount = StormConfigUtil.getIntValue(this.config, LofConfKey.MIN_DATA_COUNT, 1);
        int maxDataCount = StormConfigUtil.getIntValue(this.config, LofConfKey.MAX_DATA_COUNT, 1);
        int updateInterval = StormConfigUtil.getIntValue(this.config, LofConfKey.UPDATE_INTERVAL, 1);
        double threshold = Double.parseDouble(StormConfigUtil.getStringValue(this.config, LofConfKey.NOTITY_THRESHOLD, "1"));
        // DRPCFunction用設定値読み込み
        String function = StormConfigUtil.getStringValue(this.config, "lof.drpc.function", "lof");
        // @formatter:on

        // 状態マージ用設定生成
        Map<String, Object> mergeConfig = new HashMap<>();
        mergeConfig.put(LofConfKey.HAS_INTERMEDIATE, hasIntermediate);
        mergeConfig.put(LofConfKey.KN, kn);
        mergeConfig.put(LofConfKey.MAX_DATA_COUNT, maxDataCount);

        // 学習データ読込Spoutを初期化
        TextReadBatchSpout spout = new TextReadBatchSpout();
        spout.setDataFilePath(dataFilePath);
        spout.setBaseFileName(baseFileName);
        spout.setFileReload(isFileReload);
        spout.setMaxBatchSize(maxBatchSize);

        // Creatorを初期化
        LofPointCreator creator = new LofPointCreator();
        creator.setDelimeter(delimeter);

        // InfinispanStateFactoryを初期化
        InfinispanLofStateFactory stateFactory = new InfinispanLofStateFactory();
        stateFactory.setTargetUri(serverUri);
        stateFactory.setTableName(tableName);
        stateFactory.setMergeInterval(mergeInterval);
        stateFactory.setLifespan(lifespan);
        stateFactory.setMergeConfig(mergeConfig);

        // Updaterを初期化
        LofUpdater updater = new LofUpdater();
        updater.setAlwaysUpdateModel(alwaysUpdate);
        updater.setHasIntermediate(hasIntermediate);
        updater.setUpdateInterval(updateInterval);
        updater.setKn(kn);
        updater.setMinDataCount(minDataCount);
        updater.setMaxDataCount(maxDataCount);
        updater.setStateName(stateBaseName);
        LofResultPrinter printer = new LofResultPrinter(threshold);
        updater.setDataNotifier(printer);
        LofModelPrinter modelPrinter = new LofModelPrinter();
        updater.setBatchNotifier(modelPrinter);

        // StateQueryを初期化
        LofQuery lofQuery = new LofQuery(stateBaseName, kn, hasIntermediate);

        // TridentTopologyにSpout/Functionを登録する。
        // 入力Streamと判定Streamの2個のStreamを保持しており、各Streamの詳細は下記の通り。
        TridentTopology topology = new TridentTopology();
        // @formatter:off
        // (入力Stream)
        // 1.TextReadBatchSpout:指定されたファイルを読み込み、1行を1メッセージとして送信
        // 
        // >(each:各メッセージに対して実行)
        // 2.LofPointCreator:受信したメッセージで受信した文字列を区切り文字で分割し、各要素をdoubleの配列としてLOFの点を生成し、送信
        //
        // >(partitionPersist:各バッチに対して実行)
        // 3.LofUpdater:受信したLOFの点のリストを用いて学習モデルを更新し、Infinispanに保存する
        // 
        // ※全体並列度を「topology.parallelismHint」の設定値に併せて設定
        TridentState lofState = topology.newStream("TextReadBatchSpout", spout)
            .each(new Fields("text"), creator, new Fields("lofpoint"))
            .partitionPersist(stateFactory, new Fields("lofpoint"), updater)
            .parallelismHint(parallelism);
        
        // (判定Stream)
        // 1.DRPCStream:DRPCリクエストを受信し、その際に指定された引数をメッセージとして送信
        // 
        // >(each:各メッセージに対して実行)
        // 2.LofPointCreator:受信したメッセージで受信した文字列を区切り文字で分割し、各要素をdoubleの配列としてLOFの点を生成し、送信
        //
        // >(stateQuery:各バッチに対して実行)
        // 3.LofQuery:受信したLOFの点に対してスコア算出を行い、結果をDRPCクライアントに返信
        topology.newDRPCStream(function)
            .each(new Fields("args"), creator, new Fields("instance"))
            .stateQuery(lofState, new Fields("instance"), lofQuery, new Fields("result"));
        // @formatter:on

        // Topology内でTupleに設定するエンティティをシリアライズ登録
        this.config.registerSerialization(LofPoint.class);
        this.config.registerSerialization(Date.class);
        return topology.build();
    }
}
