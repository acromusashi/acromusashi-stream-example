package acromusashi.stream.example.spout;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.never;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import acromusashi.stream.entity.Header;
import acromusashi.stream.entity.Message;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

/**
 * HttpGetSpoutのテストクラス
 *
 * @author kimura
 */
@RunWith(MockitoJUnitRunner.class)
public class HttpGetSpoutTest
{
    /** テスト対象 */
    private HttpGetSpout         target;

    /** テスト用のSpoutOutputCollector */
    @Mock
    private SpoutOutputCollector mockCollector;

    /** テスト用のStormConfigMap */
    @SuppressWarnings("rawtypes")
    @Mock
    private Map                  mockConfMap;

    /** テスト用のTopologyContext */
    @Mock
    private TopologyContext      mockContext;

    /** テスト用のHttpClient */
    @Mock
    private HttpClient           httpClient;

    /**
     * 初期化メソッド
     */
    @Before
    public void setUp()
    {
        String url = "http://localhost:8080/message";
        this.target = new HttpGetSpout(url);
        // 初期化処理実施
        this.target.open(this.mockConfMap, this.mockContext, this.mockCollector);
        this.target.client = this.httpClient;
    }

    /**
     * HTTPGet成功時、Collectorにメッセージが通知されることを確認する。
     *
     * @target {@link HttpGetSpout#nextTuple()}
     * @test メッセージが下流コンポーネントに送信されること
     *    condition:: HTTPGet成功時
     *    result:: メッセージが下流コンポーネントに送信されることを確認
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testTextTuple_HTTPGet成功() throws IOException
    {
        // 準備
        String getResult = "{\"contents\":\"Test Message\"}";

        Mockito.doReturn(getResult).when(this.httpClient).execute((HttpUriRequest) anyObject(),
                (ResponseHandler) anyObject());

        // 実施
        this.target.nextTuple();

        // 検証
        ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
        Mockito.verify(this.mockCollector).emit(argument.capture());

        List<Object> argList = argument.getValue();

        assertThat(argList.size(), equalTo(1));
        assertThat(argList.get(0), instanceOf(Message.class));

        Message emitResult = (Message) argList.get(0);
        Header header = emitResult.getHeader();

        assertThat(header.getType(), is("http"));
        assertThat(header.getMessageId(), notNullValue());
        assertThat((header.getTimestamp() > 0), is(true));

        assertThat(emitResult.getBody().toString(), is(getResult));
    }

    /**
     * HTTPGet成功時、Collectorにメッセージが通知されないことを確認する。
     *
     * @target {@link HttpGetSpout#nextTuple()}
     * @test メッセージが下流コンポーネントに送信されないこと
     *    condition:: HTTPGet失敗時
     *    result:: メッセージが下流コンポーネントに送信されないことを確認
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testTextTuple_HTTPGet失敗() throws IOException
    {
        // 準備
        Mockito.doThrow(new IOException()).when(this.httpClient).execute(
                (HttpUriRequest) anyObject(), (ResponseHandler) anyObject());

        // 実施
        this.target.nextTuple();

        // 検証
        Mockito.verify(this.mockCollector, never()).emit((List<Object>) anyObject());
    }
}
