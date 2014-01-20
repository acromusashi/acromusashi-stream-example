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
package acromusashi.stream.example.spout;

import java.io.IOException;

import acromusashi.stream.component.rabbitmq.RabbitmqCommunicateException;
import acromusashi.stream.component.rabbitmq.spout.MessageKeyExtractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * メッセージ（JSON形式）からキー項目を抽出するクラス。<br>
 * JSON要素"header"の中に含まれるJSON要素"messageKey"の値をキーとして使用する。
 * 
 * @author acromusashi
 */
public class JsonExtractor implements MessageKeyExtractor
{
    /** serialVersionUID */
    private static final long        serialVersionUID = -1L;

    /** 親キー */
    private static final String      PARENT_KEY       = "header";

    /** 子キー */
    private static final String      CHILD_KEY        = "messageKey";

    /** Jsonツリー生成用のマッパー */
    protected transient ObjectMapper mapper;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public JsonExtractor()
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String extractMessageKey(Object target) throws RabbitmqCommunicateException
    {
        // ObjectMapperはシリアライズ不可のため、必要になったタイミングで生成する。
        if (this.mapper == null)
        {
            this.mapper = new ObjectMapper();
        }

        JsonNode rootJson;
        try
        {
            // メッセージ抽出対象をJsonNodeに変換する。（JsonNode_A）
            rootJson = this.mapper.readTree(target.toString());
        }
        catch (IOException ex)
        {
            throw new RabbitmqCommunicateException(ex);
        }

        // JsonNode_Aから親キーに対応する要素を抽出する（JsonNode_B）
        JsonNode parentJson = rootJson.get(PARENT_KEY);
        if (parentJson == null)
        {
            String message = "Parent Value is not exist.";
            throw new RabbitmqCommunicateException(message);
        }

        // JsonNode_Bから子キーに対応する要素を抽出する
        JsonNode childJson = parentJson.get(CHILD_KEY);
        if (childJson == null)
        {
            String message = "Child Value is not exist.";
            throw new RabbitmqCommunicateException(message);
        }

        return childJson.textValue();
    }
}
