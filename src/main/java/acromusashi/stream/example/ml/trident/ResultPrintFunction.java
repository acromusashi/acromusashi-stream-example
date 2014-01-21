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

import java.text.MessageFormat;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import acromusashi.stream.ml.loganalyze.ApacheLog;

/**
 * 受信したApacheのログEntityの内容を出力するFunction
 * 
 * @author kimura
 */
public class ResultPrintFunction extends BaseFunction
{
    /** serialVersionUID */
    private static final long   serialVersionUID = -6332711183777478101L;

    /** Logger */
    private static final Logger logger           = LoggerFactory.getLogger(ResultPrintFunction.class);

    /**
     * パラメータを指定せずにインスタンスを生成する。
     */
    public ResultPrintFunction()
    {}

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context)
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector)
    {
        ApacheLog statistics = (ApacheLog) tuple.getValueByField("average");
        String logFormat = "Statistics Result: Host={0}, DataCount={1}, MaxChangeFindScore={2}, ResponseTimeSum={3}, SizeSum={4}";
        logger.info(MessageFormat.format(logFormat, statistics.getKey(), statistics.getCount(),
                statistics.getAnomalyScore(), statistics.getTimeSum(), statistics.getSizeSum()));
    }
}
