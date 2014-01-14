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
package acromusashi.stream.example.bolt;

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import acromusashi.stream.bolt.MessageBolt;
import acromusashi.stream.entity.Message;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

/**
 * 受信した共通メッセージをRedisに格納するBolt<br/>
 * 
 * @author kimura
 */
public class RedisBolt extends MessageBolt
{
    /** serialVersionUID */
    private static final long   serialVersionUID = 7742873990972648063L;

    /** redishost */
    private String              redisHost;

    /** Redisにアクセスするプールオブジェクト */
    private transient JedisPool jedisPool;

    /** Redisにアクセスするクライアントオブジェクト */
    private transient Jedis     jedisClient;

    /**
     * コンストラクタ
     * 
     * @param redisHost Redis投入先ホスト
     */
    public RedisBolt(String redisHost)
    {
        this.redisHost = redisHost;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        super.prepare(stormConf, context, collector);
        this.jedisPool = new JedisPool(this.redisHost);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onMessage(Message message) throws Exception
    {
        if (this.jedisClient == null)
        {
            this.jedisClient = this.jedisPool.getResource();
        }

        this.jedisClient.set(message.getHeader().getMessageId(), message.getBody().toString());
    }
}
