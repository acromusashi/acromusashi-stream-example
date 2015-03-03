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
package acromusashi.stream.example.topology.config;

import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.entity.StreamMessage;
import acromusashi.stream.spout.AmBaseSpout;
import backtype.storm.task.TopologyContext;

/**
 * 設定ファイル再読み込みを確認するテスト用Spout
 * 
 * @author kimura
 */
public class ConfigReloadSpout extends AmBaseSpout
{
    /** serialVersionUID */
    private static final long   serialVersionUID = 7811619002547283992L;

    /** Logger */
    private static final Logger logger           = LoggerFactory.getLogger(ConfigReloadSpout.class);

    /** メッセージ生成インターバルのデフォルト値 */
    private static final long   DEFAULT_INTERVAL = 1000L;

    /** メッセージ生成インターバル */
    private long                interval         = DEFAULT_INTERVAL;

    /**
     * Default Constructor.
     */
    public ConfigReloadSpout()
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void onOpen(Map conf, TopologyContext context)
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onNextTuple()
    {
        try
        {
            TimeUnit.MILLISECONDS.sleep(this.interval);
        }
        catch (InterruptedException ex)
        {
            return;
        }

        String key = RandomStringUtils.randomAlphanumeric(10);
        String message = RandomStringUtils.randomAlphanumeric(10);

        StreamMessage streamMessage = new StreamMessage();
        streamMessage.addField("Message", message);

        emit(streamMessage, key, key);
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
