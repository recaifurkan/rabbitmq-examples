package shuaicj.example.rabbitmq.demo03.pubsub;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Demo03Consumer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String EX = "demo03-exchange";
    private static final String Q1 = "demo03-queue-1";
    private static final String Q2 = "demo03-queue-2";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection(Address.parseAddresses(CONN_STRING));

        Channel channel1 = connection.createChannel();
        Channel channel2 = connection.createChannel();

        channel1.exchangeDeclare(EX, BuiltinExchangeType.FANOUT);
        channel1.queueDeclare(Q1, false, false, false, null);
        channel1.queueDeclare(Q2, false, false, false, null);
        channel1.queueBind(Q1, EX, "");
        channel1.queueBind(Q2, EX, "");

        Consumer consumer1 = new DefaultConsumer(channel1) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) {
                log.info("Consumer1 - Message received: " + new String(body));
            }
        };

        Consumer consumer2 = new DefaultConsumer(channel2) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) {
                log.info("Consumer2 - Message received: " + new String(body));
            }
        };

        channel1.basicConsume(Q1, true, consumer1);
        channel2.basicConsume(Q2, true, consumer2);
    }
}
