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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Date;
import java.util.List;

import jp.co.acroquest.endosnipe.common.entity.ItemType;
import jp.co.acroquest.endosnipe.communicator.TelegramUtil;
import jp.co.acroquest.endosnipe.communicator.accessor.ConnectNotifyAccessor;
import jp.co.acroquest.endosnipe.communicator.accessor.ResourceNotifyAccessor;
import jp.co.acroquest.endosnipe.communicator.entity.Body;
import jp.co.acroquest.endosnipe.communicator.entity.ConnectNotifyData;
import jp.co.acroquest.endosnipe.communicator.entity.Header;
import jp.co.acroquest.endosnipe.communicator.entity.ResponseBody;
import jp.co.acroquest.endosnipe.communicator.entity.Telegram;
import jp.co.acroquest.endosnipe.communicator.entity.TelegramConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.ml.loganalyze.ApacheLog;

import com.google.common.collect.Lists;

/**
 * DataCollectorに通知を行うクラス
 * 
 * @author hiroki
 */
public class EndoSnipeNotifier
{
    /** Logger */
    private static final Logger logger        = LoggerFactory.getLogger(EndoSnipeNotifier.class);

    /** DCへの接続先ホスト */
    protected String            dcHost;

    /** DCへの接続先ポート */
    protected int               dcPort;

    /** DataCollectorへ通知する際のメッセージ名称 */
    protected String            messageName;

    /** DataCollectorへ通知する際のAgent名称 */
    protected String            agentName;

    /** タイムアウト時間(ms) */
    protected int               timeout;

    /** 初回の送信かどうかを示すフラグ */
    private boolean             firstTimeSend = true;

    /** DataCollectorへの接続ストリーム */
    private OutputStream        output;

    /** DataCollectorへの接続ソケット */
    private Socket              connectSocket;

    /**
     * 通知用パラメータを指定してインスタンスを生成する。
     * 
     * @param dcHost DCへの接続先ホスト
     * @param dcPort DCへの接続先ポート
     * @param messageName DataCollectorへ通知する際のメッセージ名称
     * @param agentName DataCollectorへ通知する際のAgent名称
     * @param timeout タイムアウト時間(ms)
     */
    public EndoSnipeNotifier(String dcHost, int dcPort, String messageName, String agentName,
            int timeout)
    {
        this.dcHost = dcHost;
        this.dcPort = dcPort;
        this.messageName = messageName;
        this.agentName = agentName;
        this.timeout = timeout;
    }

    /**
     * 性能情報を通知する。
     * 
     * @param message 性能メッセージ
     */
    public void sendMessage(ApacheLog message)
    {
        if (this.firstTimeSend)
        {

            this.connectSocket = new Socket();

            try
            {
                // 接続先ホストアドレスとポートを指定してサーバに接続する
                InetSocketAddress socketAddress = new InetSocketAddress(this.dcHost, this.dcPort);
                this.connectSocket.connect(socketAddress, this.timeout);
            }
            catch (IOException ioExc)
            {
                logger.error("接続に失敗しました。", ioExc);
            }

            this.output = null;
            try
            {
                this.output = this.connectSocket.getOutputStream();
            }
            catch (IOException ex)
            {
                logger.error("ストリーム生成に失敗しました。", ex);
            }

            this.firstTimeSend = false;

            ConnectNotifyData data = new ConnectNotifyData();
            data.setAgentName(this.agentName);
            data.setKind(ConnectNotifyData.KIND_JAVELIN);

            //            data.setPurpose(purpose);
            Telegram telegram = ConnectNotifyAccessor.createTelegram(data);

            try
            {
                send(this.output, telegram);
            }
            catch (IOException ex)
            {
                logger.error("接続通知電文の送信に失敗しました。", ex);
            }
        }

        Date logDateTime = message.getRecordedTime();
        long logTime = logDateTime.getTime();

        try
        {
            long responseTime = message.getAverageTime();
            //message.getKey()で得られるのは、log-analy1やlog-accum1のサーバ名。averageを表示する。
            sendResourceResponse(this.output, this.messageName + message.getKey()
                    + ":ResponseTime average", Long.toString(responseTime), logTime);

            long size = message.getSizeSum();
            //sumを表示する。
            sendResourceResponse(this.output, this.messageName + message.getKey() + ":Size sum",
                    Long.toString(size), logTime);
        }
        catch (IOException ioExc)
        {
            logger.error("リソース通知電文の送信に失敗しました。", ioExc);
        }
    }

    /**
     * 指定した情報をDataCollectorに通知する。
     * 
     * @param output 出力Stream
     * @param itemName Item名称
     * @param value Item値
     * @param logTime 送信時刻
     * @throws IOException 送信失敗時
     */
    private void sendResourceResponse(OutputStream output, String itemName, String value,
            long logTime) throws IOException
    {
        Telegram resourceTelgram = new Telegram();
        Header objHeader = new Header();
        objHeader.setByteRequestKind(TelegramConstants.BYTE_REQUEST_KIND_RESPONSE);
        objHeader.setByteTelegramKind(TelegramConstants.BYTE_TELEGRAM_KIND_RESOURCENOTIFY);
        resourceTelgram.setObjHeader(objHeader);

        Body body = ResourceNotifyAccessor.makeResourceResponseBody(itemName, value,
                ItemType.ITEMTYPE_STRING);

        ResponseBody timeBody = ResourceNotifyAccessor.makeTimeBody(logTime);

        Body[] objBody = Lists.newArrayList(timeBody, body).toArray(new Body[0]);
        resourceTelgram.setObjBody(objBody);
        send(output, resourceTelgram);
    }

    /**
     * 指定した電文をDataCollectorに送信する。
     * 
     * @param output 出力Stream
     * @param telegram 送信電文
     * @throws IOException 送信失敗時
     */
    private void send(OutputStream output, Telegram telegram) throws IOException
    {
        List<byte[]> connectTelegramList = TelegramUtil.createTelegram(telegram);

        for (byte[] connectDataBytes : connectTelegramList)
        {
            output.write(connectDataBytes);
            output.flush();
        }
    }
}
