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

import acromusashi.stream.bolt.MessageBolt;
import acromusashi.stream.entity.Message;

/**
 * 受信した共通メッセージをそのまま捨てるBolt<br/>
 * 
 * @author kimura
 */
public class BlackHoleBolt extends MessageBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 4824580284119159163L;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public BlackHoleBolt()
    {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void onMessage(Message message) throws Exception
    {
        // Do nothing.
    }
}
