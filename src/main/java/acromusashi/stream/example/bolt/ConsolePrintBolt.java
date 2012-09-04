package acromusashi.stream.example.bolt;

import acromusashi.stream.bolt.MessageBolt;
import acromusashi.stream.entity.Message;

/**
 * 受信した共通メッセージの文字列表現をコンソールに出力するBolt<br/>
 * ClusterModeでコンソールに出力した場合、Worker.logに出力されるため、内容はログファイルを確認すること。
 * 
 * @author kimura
 */
public class ConsolePrintBolt extends MessageBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 5100460578090478268L;

    /**
     * デフォルトコンストラクタ
     */
    public ConsolePrintBolt()
    {}

    @Override
    public void onMessage(Message message) throws Exception
    {
        System.out.println(message.toString());
    }
}
