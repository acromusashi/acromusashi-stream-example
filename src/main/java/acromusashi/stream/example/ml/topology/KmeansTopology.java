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

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.ml.clustering.kmeans.KmeansCreator;
import acromusashi.stream.ml.clustering.kmeans.KmeansQuery;
import acromusashi.stream.ml.clustering.kmeans.KmeansUpdater;
import acromusashi.stream.ml.clustering.kmeans.entity.KmeansDataSet;
import acromusashi.stream.ml.clustering.kmeans.entity.KmeansPoint;
import acromusashi.stream.ml.clustering.kmeans.entity.KmeansResult;
import acromusashi.stream.ml.clustering.kmeans.state.InfinispanKmeansStateFactory;
import acromusashi.stream.ml.common.notify.DebugLogPrinter;
import acromusashi.stream.ml.common.spout.WatchTextBatchSpout;
import acromusashi.stream.topology.BaseTridentTopology;
import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * KMeansクラスタリングを行うTopology<br>
 * ファイルから点データを読み込み、KMeans++アルゴリズムを使用してクラスタリングを行う。<br>
 * クラスタリングを行った結果の学習データをInfinispanに保存する。<br>
 * 設定時間間隔で学習モデルのマージを行い、複数スレッド間で発生した差分の補正を行う。<br>
 * DRPCで判定用データを受信し、クラスタリングを行った結果を返す。<br>
 * <br>
 * Topologyの動作フローは下記の通り。<br>
 * 入力Stream
 * <ol>
 * <li>TextReadBatchSpoutにてファイルを読み込み、1行を1データとして分割する。</li>
 * <li>KmeansCreatorにてKMeansクラスタリング用データに変換する。</li>
 * <li>KmeansUpdaterにてKMeansクラスタリング用データを学習モデルに反映し、学習モデルの更新を行う。</li>
 * </ol>
 * 判定Stream
 * <ol>
 * <li>DRPCSpoutにてDRPCクライアントから送信された判定用データを受け取る。</li>
 * <li>KmeansCreatorにてKMeansクラスタリング用データに変換する。</li>
 * <li>KmeansQueryにて学習モデルを用いてクラスタリング判定を行い、結果を返す。</li>
 * </ol>
 * 
 * yamlファイルから読み込む設定値については「KmeansTopology.yaml」参照。
 * 
 * @author kimura
 */
public class KmeansTopology extends BaseTridentTopology
{
    /**
     * Topology名称、Storm設定を指定してインスタンスを生成する。
     * 
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public KmeansTopology(String topologyName, Config config)
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
            System.out.println("Usage: java " + KmeansTopology.class.getName()
                    + " ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        KmeansTopology kmeansTopology = new KmeansTopology("KmeansTopology", conf);
        StormTopology topology = kmeansTopology.buildTridentTopology();
        kmeansTopology.submitTopology(topology, isLocal);
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
        String dataFilePath = StormConfigUtil.getStringValue(this.config, "kmeans.datafilepath", null);
        String baseFileName = StormConfigUtil.getStringValue(this.config, "kmeans.datafilebasename", null);
        // StateFactory用設定値読み込み
        String stateServers = StormConfigUtil.getStringValue(this.config, "kmeans.stateservers", null);
        String cacheName = StormConfigUtil.getStringValue(this.config, "kmeans.cachename", null);
        int mergeInterval = StormConfigUtil.getIntValue(this.config, "kmeans.merge.interval.secs", 0);
        int lifespan = StormConfigUtil.getIntValue(this.config, "kmeans.lifespan.secs", 0);
        // Creator用設定値読み込み
        String delimeter = StormConfigUtil.getStringValue(this.config, "kmeans.delimeter", ",");
        // Updater用設定値読み込み
        String stateBaseName = StormConfigUtil.getStringValue(this.config, "kmeans.statebasename", null);
        int clusterNum = StormConfigUtil.getIntValue(this.config, "kmeans.clusternum", 2);
        // DRPCFunction用設定値読み込み
        String function = StormConfigUtil.getStringValue(this.config, "kmeans.drpc.function", "kmeans");
        // @formatter:on

        // 学習データ読込Spoutを初期化
        WatchTextBatchSpout spout = new WatchTextBatchSpout();
        spout.setDataFilePath(dataFilePath);
        spout.setBaseFileName(baseFileName);

        // Creatorを初期化
        KmeansCreator creator = new KmeansCreator();
        creator.setDelimeter(delimeter);

        // InfinispanStateFactoryを初期化
        InfinispanKmeansStateFactory stateFactory = new InfinispanKmeansStateFactory();
        stateFactory.setServers(stateServers);
        stateFactory.setCacheName(cacheName);
        stateFactory.setMergeInterval(mergeInterval);
        stateFactory.setLifespan(lifespan);

        // Updaterを初期化
        KmeansUpdater updater = new KmeansUpdater();
        updater.setStateName(stateBaseName);
        updater.setClusterNum(clusterNum);
        DebugLogPrinter<KmeansResult> resultPrinter = new DebugLogPrinter<>("KmeansResult=");
        updater.setDataNotifier(resultPrinter);
        DebugLogPrinter<KmeansDataSet> modelPrinter = new DebugLogPrinter<>("KmeansDataSet=");
        updater.setBatchNotifier(modelPrinter);

        // StateQueryを初期化
        KmeansQuery kmeansQuery = new KmeansQuery(stateBaseName);

        // 以下の順でTridentTopologyにSpout/Functionを登録する。
        // 入力Streamと判定Streamの2個のStreamを保持しており、各Streamの詳細は下記の通り。
        TridentTopology topology = new TridentTopology();
        // @formatter:off
        // (入力Stream)
        // 1.TextReadBatchSpout:指定されたファイルを読み込み、1行を1メッセージとして送信
        // 
        // >(each:各メッセージに対して実行)
        // 2.KmeansCreator:受信したメッセージで受信した文字列を区切り文字で分割し、各要素をdoubleの配列としてKMeansの点を生成し、送信
        //
        // >(partitionPersist:各バッチに対して実行)
        // 3.KmeansUpdater:受信したKMeansの点のリストを用いて学習モデルを更新し、Infinispanに保存する
        // 
        // ※全体並列度を「topology.parallelismHint」の設定値に併せて設定
        TridentState kmeansState = topology.newStream("TextReadBatchSpout", spout)
            .each(new Fields("text"), creator, new Fields("kmeanspoint"))
            .partitionPersist(stateFactory, new Fields("kmeanspoint"), updater)
            .parallelismHint(parallelism);
        
        // (判定Stream)
        // 1.DRPCStream:DRPCリクエストを受信し、その際に指定された引数をメッセージとして送信
        // 
        // >(each:各メッセージに対して実行)
        // 2.KmeansCreator:受信したメッセージで受信した文字列を区切り文字で分割し、各要素をdoubleの配列としてKMeansの点を生成し、送信
        //
        // >(stateQuery:各バッチに対して実行)
        // 3.KmeansQuery:受信したKmeansの点に対してクラスタリングを行い、下記の情報を結果としてDRPCクライアントに返信
        //   a.クラスタリングされたクラスタID
        //   b.クラスタリングされたクラスタIDの中心点
        //   c.投入したデータと中心点の距離
        topology.newDRPCStream(function)
            .each(new Fields("args"), creator, new Fields("instance"))
            .stateQuery(kmeansState, new Fields("instance"), kmeansQuery, new Fields("result"));
        // @formatter:on

        // Topology内でTupleに設定するエンティティをシリアライズ登録
        this.config.registerSerialization(KmeansPoint.class);
        return topology.build();
    }
}
