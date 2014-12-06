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

import java.text.MessageFormat;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.bolt.BaseConfigurationBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 受信した単語の出現回数をカウントするBolt
 * <ul>
 * <li>インプット 単語 例)「How」</li>
 * <li>Boltの処理 単語の出現回数をカウントする</li>
 * <li>アウトプット 単語と出現回数 例）「How 5」</li>
 * </ul>
 *
 * @author kimura
 */
public class WordCountBolt extends BaseConfigurationBolt
{
    /** serialVersionUID */
    private static final long    serialVersionUID = 9080948772140456741L;

    /** logger */
    private static final Logger  logger           = LoggerFactory.getLogger(WordCountBolt.class);

    /** 単語出現回数カウンタ */
    private Map<String, Integer> counts           = new TreeMap<String, Integer>();

    /** 結果出力インターバルデフォルト値 */
    private static final long    DEFAULT_INTERVAL = 100;

    /** 結果出力インターバル */
    private long                 interval         = DEFAULT_INTERVAL;

    /** 受信メッセージ数 */
    private long                 receiveCount;

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input)
    {
        // 単語出現回数カウンタからカウンタを取得
        String word = input.getStringByField("word");
        word = word.toLowerCase();
        Integer count = this.counts.get(word);

        // カウンタ値が存在しない場合は値を0で初期化
        if (count == null)
        {
            count = 0;
        }

        count++;

        // 結果を単語出現回数カウンタに反映
        this.counts.put(word, count);

        this.receiveCount++;

        if (this.receiveCount % this.interval == 0)
        {
            String logFormat = "WordCount Result. : CountResult={0}";
            logger.warn(MessageFormat.format(logFormat, this.counts.toString()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0)
    {
        // 下流Boltは存在しないため設定しない。
    }

    /**
     * @param interval セットする interval
     */
    public void setInterval(long interval)
    {
        this.interval = interval;
    }

}
