package acromusashi.stream.example.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import acromusashi.stream.constants.FieldName;
import acromusashi.stream.entity.Header;
import acromusashi.stream.entity.Message;
import acromusashi.stream.spout.BaseConfigurationSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 一定間隔ごとに共通メッセージをBoltに送信するSpout。
 * 
 * @author tsukano
 */
public class PeriodicalMessageGenSpout extends BaseConfigurationSpout
{
    /** serialVersionUID */
    private static final long   serialVersionUID = -237111294339742815L;

    /** logger */
    private static final Logger logger           = Logger.getLogger(PeriodicalMessageGenSpout.class);

    /** 送信カウンタ */
    private int                 counter          = 0;

    /**
     * デフォルトコンストラクタ
     */
    public PeriodicalMessageGenSpout()
    {}

    /**
     * 一定間隔ごとにデータをBoltに送信する
     */
    @Override
    public void nextTuple()
    {
        this.counter++;
        Header header = new Header();
        header.setMessageId(UUID.randomUUID().toString());
        header.setTimestamp(System.currentTimeMillis());
        header.setSource("192.168.0.1");
        header.setType("message");
        Message message = new Message();
        message.setHeader(header);

        List<Object> list = new ArrayList<Object>();
        list.add("Counter:");
        list.add(this.counter);
        message.setBody(list);

        getCollector().emit(new Values(message));

        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException iex)
        {
            if (logger.isDebugEnabled() == true)
            {
                logger.debug("Occur interrupt. Ignore interrupt.", iex);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(FieldName.MESSAGE));
    }
}
