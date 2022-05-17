package cn.microboat.rocketmqdemo.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;

/**
 * 可过滤消费者
 *
 * @author zhouwei
 */
public class FilterConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test");
        consumer.setNamesrvAddr("localhost:9876");

        // 可以同时写 subscribe，但只有最后一个生效

        // 默认支持 MessageSelector.byTag
        consumer.subscribe("TopicTest2", MessageSelector.byTag("Tag1"));

        // 需要在 broker.conf 中写入配置 enablePropertyFilter=true，才能使用 MessageSelector.bySql
        consumer.subscribe("TopicTest2", MessageSelector.bySql("a between 0 and 3"));

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, content) -> {
            msgs.forEach(msg -> {
                try {
                    System.out.println(new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
    }
}
