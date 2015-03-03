package acromusashi.stream.example.topology.config;

import java.text.MessageFormat;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.bolt.AmBaseBolt;
import acromusashi.stream.entity.StreamMessage;
import backtype.storm.task.TopologyContext;

/**
 * 設定ファイル再読み込みを確認するテスト用Bolt
 * 
 * @author kimura
 */
public class ConfigReloadBolt extends AmBaseBolt
{
    /** serialVersionUID */
    private static final long   serialVersionUID = 7811619002547283992L;

    /** Logger */
    private static final Logger logger           = LoggerFactory.getLogger(ConfigReloadBolt.class);

    /**
     * Default Constructor.
     */
    public ConfigReloadBolt()
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onPrepare(Map stormConf, TopologyContext context)
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecute(StreamMessage input)
    {
        String msgFormat = "Message received. : Message={0}";
        logger.info(MessageFormat.format(msgFormat, input.getField("Message")));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onUpdate(Map<String, Object> reloadedConfig)
    {
        String msgFormat = "Config uploaded. : Config={0}";
        logger.info(MessageFormat.format(msgFormat, reloadedConfig.toString()));
    }
}
