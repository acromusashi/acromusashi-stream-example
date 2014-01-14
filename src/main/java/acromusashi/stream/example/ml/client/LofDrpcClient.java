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
 * LofTopologyに対してデータの投入を行い、結果を受け取るDRPCクライアント
 * 
 * @author kimura
 */
public class LofDrpcClient
{
    /** デフォルトポート */
    public static final int DEFAULT_DRPCPORT = 3772;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public LofDrpcClient()
    {}

    /**
     * プログラムエントリポイント<br/>
     * <br/>
     * 下記の引数/オプションを使用する。<br/>
     * <ul>
     * <li>-c LOFTopology用設定ファイルパス(必須入力）</li>
     * <li>-d LOF Data(必須入力、学習用データと同形式で投入)</li>
     * <li>-h ヘルプ表示</li>
     * </ul>
     * 
     * @param args 起動引数
     */
    public static void main(String... args)
    {
        LofDrpcClient client = new LofDrpcClient();
        client.startSendRequest(args);
    }

    /**
     * LofTopologyに対するDRPCリクエスト送信を開始する。
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
            help.printHelp(LofDrpcClient.class.getName(), cliOptions, true);
            return;
        }

        if (commandLine.hasOption("h"))
        {
            // ヘルプオプションが指定されていた場合にはヘルプを表示して終了
            help.printHelp(LofDrpcClient.class.getName(), cliOptions, true);
            return;
        }

        // コマンドラインから設定値を取得
        String confPath = commandLine.getOptionValue("c");
        String lofData = commandLine.getOptionValue("d");

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
        String drpcFunction = StormConfigUtil.getStringValue(stormConfig, "lof.drpc.function",
                "lof");

        double lofScore = 0.0d;

        try
        {
            lofScore = sendRequest(drpcHost, drpcPort, drpcFunction, lofData);
        }
        catch (TException | DRPCExecutionException | IOException ex)
        {
            // 送信失敗した場合は終了する
            ex.printStackTrace();
            return;
        }

        System.out.println("LOF Data=" + lofData + ", LOFScore=" + lofScore);
    }

    /**
     * LOF向けDRPCリクエストを送信し、LOFスコアを取得する。
     * 
     * @param drpcHost DRPCホスト
     * @param drpcPort DRPCポート
     * @param drpcFunction 呼び出し機能名称
     * @param lofData 投入LOFデータ
     * @return LOFスコア
     * @throws DRPCExecutionException DRPCリクエスト失敗時
     * @throws TException DRPCリクエスト失敗時
     * @throws IOException 結果パース失敗時
     */
    public double sendRequest(String drpcHost, int drpcPort, String drpcFunction, String lofData)
            throws TException, DRPCExecutionException, IOException
    {
        DRPCClient client = new DRPCClient(drpcHost, drpcPort);
        String drpcResult = client.execute(drpcFunction, lofData);

        // 以下のような形式で返るため、パースを行う。
        // [["84.406 129.347 50.527","LofPoint[dataId=7b6f1200-7117-4231-bc95-32c448237f5b,
        // dataPoint={84.406,129.347,50.527},kDistance=0.0,kDistanceNeighbor=<null>,lrd=0.0,judgeDate=Tue Nov 26 18:56:00 JST 2013]",
        // "{\"lofScore\":1.924027244063857,\"lofPoint\":{\"dataId\":\"7b6f1200-7117-4231-bc95-32c448237f5b\",
        // \"dataPoint\":[84.406,129.347,50.527],\"kDistance\":0.0,\"kDistanceNeighbor\":null,\"lrd\":0.0,\"judgeDate\":1385459760464}}"]]

        ObjectMapper mapper = new ObjectMapper();
        JsonNode baseNode = mapper.readTree(drpcResult);
        String lofResultStr = baseNode.get(0).get(2).asText();
        JsonNode lofResultTree = mapper.readTree(lofResultStr);
        double lofScore = lofResultTree.get("lofScore").asDouble();
        return lofScore;
    }

    /**
     * コマンドライン解析用のオプションを生成
     * 
     * @return コマンドライン解析用のオプション
     */
    public static Options createOptions()
    {
        Options cliOptions = new Options();

        // LofTopology定義ファイルパスオプション
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("LofTopology Conf Path");
        OptionBuilder.withDescription("LofTopology Conf Path");
        OptionBuilder.isRequired(true);
        Option confPathOption = OptionBuilder.create("c");

        // LOF投入データオプション
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("LOF Data");
        OptionBuilder.withDescription("LOF Data");
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
