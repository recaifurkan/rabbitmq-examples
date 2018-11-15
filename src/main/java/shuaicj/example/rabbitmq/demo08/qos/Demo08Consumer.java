package shuaicj.example.rabbitmq.demo08.qos;

import java.io.IOException;
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

@Slf4j
public class Demo08Consumer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String Q = "demo08-queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection(Address.parseAddresses(CONN_STRING));

        Channel channel1 = connection.createChannel();
        Channel channel2 = connection.createChannel();

        boolean durable = true;
        channel1.queueDeclare(Q, durable, false, false, null);
        channel2.queueDeclare(Q, durable, false, false, null);

        channel1.basicQos(2);
        channel2.basicQos(3);

        Consumer consumer1 = new DefaultConsumer(channel1) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    doWork("Consumer1", body, 1000);
                } finally {
                    getChannel().basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        Consumer consumer2 = new DefaultConsumer(channel2) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    doWork("Consumer2", body, 1);
                } finally {
                    getChannel().basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        boolean autoAck = false;
        channel1.basicConsume(Q, autoAck, consumer1);
        channel2.basicConsume(Q, autoAck, consumer2);
    }

    private static void doWork(String name, byte[] body, long sleep) {
        String message = new String(body);
        log.info(name + " - Message received: " + message);
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info(name + " - Work done! " + message);
    }
}
