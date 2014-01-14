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
package acromusashi.stream.example.ml.trident;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import acromusashi.stream.ml.loganalyze.ApacheLog;

/**
 * 受信した性能情報を基にDataCollectorに通知を行うFunction
 * 
 * @author hiroki
 */
public class EndoSnipeNotifyFunction extends BaseFunction
{
    /** serialVersionUID */
    private static final long           serialVersionUID     = -6332711183777478101L;

    /** Logger */
    private static final Logger         logger               = LoggerFactory.getLogger(EndoSnipeNotifyFunction.class);

    /** 「DCへの接続先ホスト」デフォルト値 */
    protected static final String       DEFAULT_DC_HOST      = "localhost";

    /** 「DCへの接続先ポート」デフォルト値 */
    protected static final int          DEFAULT_DC_PORT      = 18000;

    /** 「メッセージ名称」デフォルト値 */
    protected static final String       DEFAULT_MESSAGE_NAME = "/Trident/Performance/";

    /** 「Agent名称」デフォルト値 */
    protected static final String       DEFAULT_AGENT_NAME   = "/Performance";

    /** 「タイムアウト時間」デフォルト値 */
    protected static final int          DEFAULT_TIMEOUT      = 10000;

    /** DCへの接続先ホスト */
    protected String                    dcHost               = DEFAULT_DC_HOST;

    /** DCへの接続先ポート */
    protected int                       dcPort               = DEFAULT_DC_PORT;

    /** DataCollectorへ通知する際のメッセージ名称 */
    protected String                    messageName          = DEFAULT_MESSAGE_NAME;

    /** DataCollectorへ通知する際のAgent名称 */
    protected String                    agentName            = DEFAULT_AGENT_NAME;

    /** タイムアウト時間(ms) */
    protected int                       timeout              = DEFAULT_TIMEOUT;

    /** DataCollectorへの通知オブジェクト */
    private transient EndoSnipeNotifier notifier;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public EndoSnipeNotifyFunction()
    {}

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context)
    {
        this.notifier = new EndoSnipeNotifier(dcHost, dcPort, messageName, agentName, timeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector)
    {
        ApacheLog average = (ApacheLog) tuple.getValueByField("average");

        try
        {
            this.notifier.sendMessage(average);
            logger.info("sender.average:" + average);
        }
        catch (Exception e)
        {
            collector.reportError(e);
            logger.info("collector.reportErrot");
        }
    }

    /**
     * @param dcHost the dcHost to set
     */
    public void setDcHost(String dcHost)
    {
        this.dcHost = dcHost;
    }

    /**
     * @param dcPort the dcPort to set
     */
    public void setDcPort(int dcPort)
    {
        this.dcPort = dcPort;
    }

    /**
     * @param messageName the messageName to set
     */
    public void setMessageName(String messageName)
    {
        this.messageName = messageName;
    }

    /**
     * @param agentName the agentName to set
     */
    public void setAgentName(String agentName)
    {
        this.agentName = agentName;
    }
}
