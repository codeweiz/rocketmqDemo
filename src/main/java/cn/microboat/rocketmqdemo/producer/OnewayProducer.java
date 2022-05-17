package cn.microboat.rocketmqdemo.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * 单向传输
 *
 * @author zhouwei
 */
public class OnewayProducer {

    public static void main(String[] args) throws RemotingException, InterruptedException, MQClientException, UnsupportedEncodingException {
        DefaultMQProducer producer = new DefaultMQProducer("test");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        for (int i = 0; i < 100; i++) {
            Message msg = new Message("TopicTest", "TagA", ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.sendOneway(msg);
        }

        Thread.sleep(5 * 1000);
        producer.shutdown();
    }
}
