package cn.microboat.rocketmqdemo.producer;

import cn.microboat.rocketmqdemo.pojo.ListSplitter;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhouwei
 */
public class BatchProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer("test");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        // 当消息列表 messages 小于 1MiB时，可以直接 send
        String topic = "batchTest";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "TagA", "OrderId001", "hello world 0".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderId002", "hello world 1".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderId003", "hello world 2".getBytes()));

        SendResult sendResult = producer.send(messages);
        System.out.println(sendResult);


        // 如果批量消息 大于 1MiB，需要进行分割
        messages = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            messages.add(new Message(topic, "TagA", "OrderId00" + i, ("hello world " + i).getBytes()));
        }

        ListSplitter listSplitter = new ListSplitter(messages);

        if (listSplitter.hasNext()) {
            SendResult result = producer.send(listSplitter.next());
            System.out.println(result);
        }

        producer.shutdown();
    }
}
