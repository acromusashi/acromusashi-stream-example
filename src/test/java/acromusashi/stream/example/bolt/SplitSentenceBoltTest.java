package acromusashi.stream.example.bolt;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * SplitSentenceBoltのテストクラス
 *
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class SplitSentenceBoltTest
{
    /** テスト対象 */
    private SplitSentenceBolt target;

    /** テスト用のOutputCollector */
    @Mock
    private OutputCollector   mockCollector;

    /** テスト用のStormConfigMap */
    @SuppressWarnings("rawtypes")
    @Mock
    private Map               mockConfMap;

    /** テスト用のTopologyContext */
    @Mock
    private TopologyContext   mockContext;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        this.target = new SplitSentenceBolt();
        // 初期化処理実施
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);
    }

    /**
     * 文章が分割されることを確認する。
     *
     * @target {@link SplitSentenceBolt#execute(backtype.storm.tuple.Tuple)}
     * @test 文章を分割した単語が下流コンポーネントに送信されること
     *    condition:: メッセージを受信
     *    result:: 文章を分割した単語が下流コンポーネントに送信されることを確認
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testExecute_分割成功() throws IOException
    {
        // 準備
        Tuple mockTuple = Mockito.mock(Tuple.class);
        Mockito.doReturn("Test Message").when(mockTuple).getStringByField("message");

        // 実行
        this.target.execute(mockTuple);

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector, times(2)).emit(argument.capture());
        List<List> argList = argument.getAllValues();

        assertThat(argList.size(), equalTo(2));

        assertThat(argList.get(0).get(0).toString(), is("Test"));
        assertThat(argList.get(1).get(0).toString(), is("Message"));
    }
}
