package acromusashi.stream.example;

import org.apache.camel.spring.Main;

/**
 * JSON　HTTP Requestを受信し、KestrelにJSON形式で投入するCamelプロセスを起動するサンプルプログラム<br/>
 * JVMオプション「camel.contexturi」からCamelの設定ファイルパスを取得する。<br/>
 * <br/>
 * 動作フローは下記の通り。
 * 
 * <ol>
 * <li>&lt;from uri="jetty:http:～&gt;に記述したアドレスでHTTPRequestを受信する</li>
 * <li>受信したHTTPRequestからValueを抽出する</li>
 * <li>&lt;to uri="kestrel:～&gt;に記述したアドレスに存在するKestrelに対して抽出したValueを投入する</li>
 * </ol>
 * 
 * @author otoda
 */
public final class CamelJson2KestrelMain
{
    /**
     * デフォルトコンストラクタ
     */
    public CamelJson2KestrelMain()
    {}

    /**
     * Snmpを受信し、Kestrelに投入するサンプルプログラムのプログラムエントリポイント
     * 
     * @param args 未使用
     * @throws Exception 起動失敗時
     */
    public static void main(String[] args) throws Exception
    {
        CamelJson2KestrelMain main = new CamelJson2KestrelMain();
        main.execute();
    }

    /**
     * Camelを起動する。
     * 
     * @throws Exception 起動失敗時
     */
    public void execute() throws Exception
    {
        Main main = new Main();
        String confUri = System.getProperty("camel.contexturi",
                "camel-context-example-json_kestrel.xml");
        main.setApplicationContextUri(confUri);
        main.enableHangupSupport();
        main.run();
    }
}
