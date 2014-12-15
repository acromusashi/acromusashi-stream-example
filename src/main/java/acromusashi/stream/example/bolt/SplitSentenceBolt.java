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

import org.apache.commons.lang.StringUtils;

import acromusashi.stream.bolt.BaseConfigurationBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
* 受信した英語の文章を単語単位に分割して次のBoltに送信するBolt
* <ul>
* <li>インプット 文章 例)「How are you」</li>
* <li>Boltの処理 受信メッセージに含まれる文章を単語単位に分割する</li>
* <li>アウトプット 単語単位に分割されたTuple「How」「are」「you」</li>
* </ul>
*
* @author kimura
*/
public class SplitSentenceBolt extends BaseConfigurationBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = -445961598496006743L;

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public SplitSentenceBolt()
    {}

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input)
    {
        // 文章を単語単位に分割する
        String sentence = input.getStringByField("message");
        String[] words = StringUtils.split(sentence);

        // 単語単位にTupleに分割し、次のBoltに送信する
        for (String targetWord : words)
        {
            getCollector().emit(new Values(targetWord.toLowerCase()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word"));
    }
}
