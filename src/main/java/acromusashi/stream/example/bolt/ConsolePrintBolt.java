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

import acromusashi.stream.bolt.AmBaseBolt;
import acromusashi.stream.entity.StreamMessage;
import backtype.storm.task.TopologyContext;

/**
 * 受信した共通メッセージの文字列表現をコンソールに出力するBolt<br/>
 * ClusterModeでコンソールに出力した場合、Worker.logに出力されるため、内容はログファイルを確認すること。
 * 
 * @author kimura
 */
public class ConsolePrintBolt extends AmBaseBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 5100460578090478268L;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public ConsolePrintBolt()
    {}

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void onPrepare(Map config, TopologyContext context)
    {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onExecute(StreamMessage message)
    {
        System.out.println(message.toString());
    }
}
