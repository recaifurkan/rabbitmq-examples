package shuaicj.example.rabbitmq.demo07.persistence;

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
public class Demo07Consumer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String EX = "demo07-exchange";
    private static final String Q = "demo07-queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection(Address.parseAddresses(CONN_STRING));
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EX, BuiltinExchangeType.DIRECT);

        boolean durable = true;
        channel.queueDeclare(Q, durable, false, false, null);
        channel.queueBind(Q, EX, Q);

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
