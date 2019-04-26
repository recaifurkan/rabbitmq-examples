package shuaicj.example.rabbitmq.demo10.consumer.ack.reject;

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
public class Demo10Consumer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String Q = "demo10-queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection(Address.parseAddresses(CONN_STRING));

        Channel channel1 = connection.createChannel();
        Channel channel2 = connection.createChannel();
        Channel channel3 = connection.createChannel();

        channel1.queueDeclare(Q, false, false, false, null);

        Consumer consumer1 = new DefaultConsumer(channel1) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body);
                log.info("Consumer1 - Message received: " + message);
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                log.info("Consumer1 - Ack: " + message);
            }
        };

        Consumer consumer2 = new DefaultConsumer(channel2) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body);
                log.info("Consumer2 - Message received: " + message);
                boolean requeue = false;
                getChannel().basicReject(envelope.getDeliveryTag(), requeue);
                log.info("Consumer2 - Reject and discard: " + message);
            }
        };

        Consumer consumer3 = new DefaultConsumer(channel3) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body);
                log.info("Consumer3 - Message received: " + message);
                boolean requeue = true;
                getChannel().basicReject(envelope.getDeliveryTag(), requeue);
                log.info("Consumer3 - Reject and requeue: " + message);
            }
        };

        channel1.basicQos(1);
        channel2.basicQos(1);
        channel3.basicQos(1);

        boolean autoAck = false;
        channel1.basicConsume(Q, autoAck, consumer1);
        channel2.basicConsume(Q, autoAck, consumer2);
        channel3.basicConsume(Q, autoAck, consumer3);
    }
}
