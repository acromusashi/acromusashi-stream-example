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

import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.example.bolt.JsonExtractBolt;
import acromusashi.stream.example.bolt.SplitSentenceBolt;
import acromusashi.stream.example.bolt.WordCountBolt;
import acromusashi.stream.example.spout.HttpGetSpout;
import acromusashi.stream.topology.BaseTopology;
import backtype.storm.Config;
import backtype.storm.tuple.Fields;

/**
 * HTTPリクエストを送信して取得したJSONメッセージを取得し、ワードカウントを行うTopology。<br>
 * 動作は下記の通り。<br>
 * <ol>
 * <li>HttpGetSpoutにてJSON形式のメッセージを受信する</li>
 * <li>JsonExtractBoltにてJSON形式のメッセージから文章を抽出する</li>
 * <li>SplitSentenceBoltにて単語単位に分割する</li>
 * <li>WordCountBoltにて単語単位に集計し、一定件数受信ごとに結果をログ出力する</li>
 * </ol>
 *
 * yamlファイルから読み込む設定値
 * <ul>
 * <li>http.get.targetUrl : JSON形式のメッセージを取得する対象URL</li>
 * <li>HttpGetSpout.Parallelism : HttpGetSpoutの並列度</li>
 * <li>JsonExtractBolt.Parallelism : JsonExtractBoltの並列度</li>
 * <li>SplitSentenceBolt.Parallelism : SplitSentenceBoltの並列度</li>
 * <li>WordCountBolt.Parallelism : WordCountBoltの並列度</li>
 * </ul>
 *
 * ローカル環境での実行方法<br>
 * <ol>
 * <li>testenv/wiremock-standalone.zipを展開する</li>
 * <li>展開したディレクトリに移動し、コマンド「java -jar wiremock-1.52-standalone.jar」を実行。</li>
 * </ol>
 * MessageTopologyにKestrelのアドレスを指定し、<br>
 * storm-starterプロジェクト　クラス「storm.starter.WordCountTopology」を下記の条件で実行する。
 * <ol>
 * <li>Program arguments : conf/HttpWordCountTopology.yaml true</li>
 * </ol>
 *
 * @author acromusashi
 */
public class HttpWordCountTopology extends BaseTopology
{
    /**
     * コンストラクタ
     *
     * @param topologyName Topology名称
     * @param config Storm設定オブジェクト
     */
    public HttpWordCountTopology(String topologyName, Config config)
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
            System.out.println("Usage: java " + HttpWordCountTopology.class.getName()
                    + " ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        // Topologyを起動する
        BaseTopology topology = new HttpWordCountTopology("HttpWordCountTopology", conf);
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
        String targetUrl = StormConfigUtil.getStringValue(getConfig(), "http.get.targetUrl", "");
        int spoutPara = StormConfigUtil.getIntValue(getConfig(), "HttpGetSpout.Parallelism", 1);
        int jsonPara = StormConfigUtil.getIntValue(getConfig(), "JsonExtractBolt.Parallelism", 1);
        int splitPara = StormConfigUtil.getIntValue(getConfig(), "SplitSentenceBolt.Parallelism", 1);
        int wordcountPara = StormConfigUtil.getIntValue(getConfig(), "WordCountBolt.Parallelism", 1);
        // @formatter:on

        // Topology Setting
        // Add Spout(HttpGetSpout)
        HttpGetSpout spout = new HttpGetSpout(targetUrl);
        getBuilder().setSpout("HttpGetSpout", spout, spoutPara);

        // Add Bolt(JsonExtractBolt)
        JsonExtractBolt jsonBolt = new JsonExtractBolt("contents");
        getBuilder().setBolt("JsonExtractBolt", jsonBolt, jsonPara).shuffleGrouping("HttpGetSpout");

        // Add Bolt(SplitSentenceBolt)
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        getBuilder().setBolt("SplitSentenceBolt", splitBolt, splitPara).shuffleGrouping(
                "JsonExtractBolt");

        // Add Bolt(WordCountBolt)
        WordCountBolt wordcountBolt = new WordCountBolt();
        getBuilder().setBolt("WordCountBolt", wordcountBolt, wordcountPara).fieldsGrouping(
                "SplitSentenceBolt", new Fields("word"));
    }
}
