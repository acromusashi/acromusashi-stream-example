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
package acromusashi.stream.example.ml.client;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.thrift7.TException;

import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import backtype.storm.Config;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * KMeansTopologyに対してデータの投入を行い、結果を受け取るDRPCクライアント
 * 
 * @author kimura
 */
public class KMeansDrpcClient
{
    /** デフォルトポート */
    public static final int DEFAULT_DRPCPORT = 3772;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public KMeansDrpcClient()
    {}

    /**
     * プログラムエントリポイント<br/>
     * <br/>
     * 下記の引数/オプションを使用する。<br/>
     * <ul>
     * <li>-c KMeansTopology用設定ファイルパス(必須入力）</li>
     * <li>-d KMeans Data(必須入力、学習用データと同形式で投入)</li>
     * <li>-h ヘルプ表示</li>
     * </ul>
     * 
     * @param args 起動引数
     */
    public static void main(String... args)
    {
        KMeansDrpcClient client = new KMeansDrpcClient();
        client.startSendRequest(args);
    }

    /**
     * KMeansTopologyに対するDRPCリクエスト送信を開始する。
     * 
     * @param args 起動時引数
     */
    protected void startSendRequest(String[] args)
    {
        Options cliOptions = createOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = null;
        HelpFormatter help = new HelpFormatter();

        try
        {
            commandLine = parser.parse(cliOptions, args);
        }
        catch (ParseException pex)
        {
            help.printHelp(KMeansDrpcClient.class.getName(), cliOptions, true);
            return;
        }

        if (commandLine.hasOption("h"))
        {
            // ヘルプオプションが指定されていた場合にはヘルプを表示して終了
            help.printHelp(KMeansDrpcClient.class.getName(), cliOptions, true);
            return;
        }

        // コマンドラインから設定値を取得
        String confPath = commandLine.getOptionValue("c");
        String kmeansData = commandLine.getOptionValue("d");

        Config stormConfig = null;

        try
        {
            stormConfig = StormConfigGenerator.loadStormConfig(confPath);
        }
        catch (IOException ex)
        {
            // 読み込み失敗した場合は終了する
            ex.printStackTrace();
            return;
        }

        String drpcHost = StormConfigUtil.getStringListValue(stormConfig, "drpc.servers").get(0);
        int drpcPort = StormConfigUtil.getIntValue(stormConfig, "drpc.port", DEFAULT_DRPCPORT);
        String drpcFunction = StormConfigUtil.getStringValue(stormConfig, "kmeans.drpc.function",
                "kmeans");

        String kmeanResult = null;

        try
        {
            kmeanResult = sendRequest(drpcHost, drpcPort, drpcFunction, kmeansData);
        }
        catch (TException | DRPCExecutionException | IOException ex)
        {
            // 送信失敗した場合は終了する
            ex.printStackTrace();
            return;
        }

        System.out.println("KMeans Data=" + kmeansData + ", KMeans Result=" + kmeanResult);
    }

    /**
     * KMeans向けDRPCリクエストを送信し、LOFスコアを取得する。
     * 
     * @param drpcHost DRPCホスト
     * @param drpcPort DRPCポート
     * @param drpcFunction 呼び出し機能名称
     * @param kmeansData 投入KMeansデータ
     * @return KMeans結果
     * @throws DRPCExecutionException DRPCリクエスト失敗時
     * @throws TException DRPCリクエスト失敗時
     * @throws IOException 結果パース失敗時
     */
    public String sendRequest(String drpcHost, int drpcPort, String drpcFunction, String kmeansData)
            throws TException, DRPCExecutionException, IOException
    {
        DRPCClient client = new DRPCClient(drpcHost, drpcPort);
        String drpcResult = client.execute(drpcFunction, kmeansData);

        // 以下のような形式で返るため、パースを行う。
        // [["0.3230647,0.4288709,0.2918048,0.7904559,5",
        // "KmeansPoint[label=<null>,dataPoint={0.3230647,0.4288709,0.2918048,0.7904559,5.0}]",
        // "{\"dataPoint\":[0.3230647,0.4288709,0.2918048,0.7904559,5.0],\"centroidIndex\":2,
        // \"centroid\":[0.49254449862586597,0.29060293775519647,0.19902678114457303,0.7925079800808319,5.0],\"distance\":0.23759924945376112}"]]

        ObjectMapper mapper = new ObjectMapper();
        JsonNode baseNode = mapper.readTree(drpcResult);
        String kmeanResultStr = baseNode.get(0).get(2).asText();
        JsonNode kmeanResultTree = mapper.readTree(kmeanResultStr);
        return kmeanResultTree.toString();
    }

    /**
     * コマンドライン解析用のオプションを生成
     * 
     * @return コマンドライン解析用のオプション
     */
    public static Options createOptions()
    {
        Options cliOptions = new Options();

        // KMeansTopology定義ファイルパスオプション
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("KMeansTopology Conf Path");
        OptionBuilder.withDescription("KMeansTopology Conf Path");
        OptionBuilder.isRequired(true);
        Option confPathOption = OptionBuilder.create("c");

        // LOF投入データオプション
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("KMeans Data");
        OptionBuilder.withDescription("KMeans Data");
        OptionBuilder.isRequired(true);
        Option dataOption = OptionBuilder.create("d");

        // ヘルプオプション
        OptionBuilder.withDescription("show help");
        Option helpOption = OptionBuilder.create("h");

        cliOptions.addOption(confPathOption);
        cliOptions.addOption(dataOption);
        cliOptions.addOption(helpOption);
        return cliOptions;
    }
}
