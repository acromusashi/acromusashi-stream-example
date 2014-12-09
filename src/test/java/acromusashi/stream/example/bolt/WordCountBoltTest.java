package acromusashi.stream.example.bolt;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * WordCountBoltのテストクラス
 *
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class WordCountBoltTest
{
    /** テスト対象 */
    private WordCountBolt   target;

    /** テスト用のOutputCollector */
    @Mock
    private OutputCollector mockCollector;

    /** テスト用のStormConfigMap */
    @SuppressWarnings("rawtypes")
    @Mock
    private Map             mockConfMap;

    /** テスト用のTopologyContext */
    @Mock
    private TopologyContext mockContext;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        this.target = new WordCountBolt();
        // 初期化処理実施
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);
        this.target.setInterval(1);
    }

    /**
     * Test、word、testの順で単語を受信した場合、test=2、word=1の順に値が保持されていることを確認する。
     *
     * @target {@link WordCountBolt#execute(backtype.storm.tuple.Tuple)}
     * @test test=2、word=1の順に値が保持されていること
     *    condition:: test、word、testの順で単語を受信
     *    result:: test=2、word=1の順に値が保持されていることを確認
     */
    @Test
    public void testExecute_単語カウント() throws IOException
    {
        // 準備
        Tuple mockTuple1 = Mockito.mock(Tuple.class);
        Mockito.doReturn("Test").when(mockTuple1).getStringByField("word");

        Tuple mockTuple2 = Mockito.mock(Tuple.class);
        Mockito.doReturn("word").when(mockTuple2).getStringByField("word");

        Tuple mockTuple3 = Mockito.mock(Tuple.class);
        Mockito.doReturn("test").when(mockTuple3).getStringByField("word");

        // 実行
        this.target.execute(mockTuple1);
        this.target.execute(mockTuple2);
        this.target.execute(mockTuple3);

        // 検証
        assertThat(this.target.counts.size(), equalTo(2));
        assertThat(this.target.counts.containsKey("test"), is(true));
        assertThat(this.target.counts.containsKey("word"), is(true));
        assertThat(this.target.counts.get("test"), is(2));
        assertThat(this.target.counts.get("word"), is(1));
    }
}
