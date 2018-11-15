package shuaicj.example.rabbitmq.demo11.consumer.thread.pool;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

/**
 * Check the thread name in logging messages and compare it to the logging output of other demo's consumer.
 */
@Slf4j
public class Demo11Consumer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String Q = "demo11-queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();

        Connection connection = factory.newConnection(
                Executors.newFixedThreadPool(3, new CustomizableThreadFactory("my-thread-")),
                Address.parseAddresses(CONN_STRING));

        Channel channel = connection.createChannel();

        channel.queueDeclare(Q, false, false, false, null);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) {
                log.info("Message received: " + new String(body));
            }
        };

        channel.basicConsume(Q, true, consumer);
    }

}
