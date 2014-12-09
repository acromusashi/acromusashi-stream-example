package acromusashi.stream.example.bolt;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.never;

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

import acromusashi.stream.entity.Message;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * JsonExtractBoltのテストクラス
 *
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class JsonExtractBoltTest
{
    /** テスト対象 */
    private JsonExtractBolt target;

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
        String key = "contents";
        this.target = new JsonExtractBolt(key);
        // 初期化処理実施
        this.target.prepare(this.mockConfMap, this.mockContext, this.mockCollector);
    }

    /**
     * 形式が不正なメッセージ受信時、メッセージを破棄して動作が継続することを確認する。
     *
     * @target {@link JsonExtractBolt#execute(backtype.storm.tuple.Tuple)}
     * @test メッセージが下流コンポーネントに送信されないこと
     *    condition:: 形式が不正なメッセージ受信時
     *    result:: メッセージが下流コンポーネントに送信されないことを確認
     */
    @SuppressWarnings({"unchecked"})
    @Test
    public void testExecute_形式不正() throws IOException
    {
        // 準備
        Tuple mockTuple = Mockito.mock(Tuple.class);
        Message message = new Message();
        message.setBody("Invalid Message");
        Mockito.doReturn(message).when(mockTuple).getValueByField("message");

        // 実行
        this.target.execute(mockTuple);

        // 検証
        Mockito.verify(this.mockCollector, never()).emit((List<Object>) anyObject());
    }

    /**
     * キー項目を保持しないJSONメッセージを受信時、メッセージを破棄して動作が継続することを確認する。
     *
     * @target {@link JsonExtractBolt#execute(backtype.storm.tuple.Tuple)}
     * @test メッセージが下流コンポーネントに送信されないこと
     *    condition:: キー項目を保持しないJSONメッセージ受信時
     *    result:: メッセージが下流コンポーネントに送信されないことを確認
     */
    @SuppressWarnings({"unchecked"})
    @Test
    public void testExecute_キー要素未存在() throws IOException
    {
        // 準備
        Tuple mockTuple = Mockito.mock(Tuple.class);
        Message message = new Message();
        message.setBody("{\"key\":\"Test Message\"}");
        Mockito.doReturn(message).when(mockTuple).getValueByField("message");

        // 実行
        this.target.execute(mockTuple);

        // 検証
        Mockito.verify(this.mockCollector, never()).emit((List<Object>) anyObject());

    }

    /**
     * キー項目を保持するJSONメッセージを受信時、メッセージが下流コンポーネントに送信されること。
     *
     * @target {@link JsonExtractBolt#execute(backtype.storm.tuple.Tuple)}
     * @test メッセージが下流コンポーネントに送信されること
     *    condition:: キー項目を保持しないJSONメッセージ受信時
     *    result:: メッセージが下流コンポーネントに送信されることを確認
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testExecute_正常() throws IOException
    {
        // 準備
        Tuple mockTuple = Mockito.mock(Tuple.class);
        Message message = new Message();
        message.setBody("{\"contents\":\"Test Message\"}");
        Mockito.doReturn(message).when(mockTuple).getValueByField("message");

        // 実行
        this.target.execute(mockTuple);

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());
        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(1));
        assertThat(argList.get(0), instanceOf(String.class));

        assertThat(argList.get(0).toString(), is("Test Message"));
    }
}
