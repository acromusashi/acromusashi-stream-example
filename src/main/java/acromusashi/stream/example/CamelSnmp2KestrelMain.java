package acromusashi.stream.example;

import org.apache.camel.spring.Main;

/**
 * SnmpTrapを受信し、KestrelにJSON形式で投入するCamelプロセスを起動するサンプルプログラム<br/>
 * JVMオプション「camel.contexturi」からCamelの設定ファイルパスを取得する。<br/>
 * <br/>
 * 動作フローは下記の通り。<br/>
 * 
 * <ol>
 * <li>&lt;from uri="snmp:～&gt;に記述したアドレスでSNMP4JのSNMPTrapを受信する</li>
 * <li>受信したSNMPTrapをJSON形式に変換する</li>
 * <li>&lt;to uri="kestrel:～&gt;に記述したアドレスに存在するKestrelに対して変換したTrapを投入する</li>
 * </ol>
 * 
 * @author otoda
 */
public final class CamelSnmp2KestrelMain
{
    /**
     * デフォルトコンストラクタ
     */
    public CamelSnmp2KestrelMain()
    {}

    /**
     * Twitterからつぶやきを取得し、Kestrelに投入するサンプルプログラムのプログラムエントリポイント
     * 
     * @param args 未使用
     * @throws Exception 起動失敗時
     */
    public static void main(String[] args) throws Exception
    {
        CamelSnmp2KestrelMain main = new CamelSnmp2KestrelMain();
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
                "camel-context-example-snmp_kestrel.xml");
        main.setApplicationContextUri(confUri);
        main.enableHangupSupport();
        main.run();
    }
}
