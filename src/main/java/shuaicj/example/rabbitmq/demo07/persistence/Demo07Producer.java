package shuaicj.example.rabbitmq.demo07.persistence;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import lombok.extern.slf4j.Slf4j;

/**
 * To make the messages persistent:
 *
 * 1. The durability of the exchange is not mandatory.
 * 2. The durability of the queue is mandatory, and it must exist already when you produce the messages.
 * 3. The messages must be persistent, by setting the properties.
 */
@Slf4j
public class Demo07Producer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String EX = "demo07-exchange";
    private static final String Q = "demo07-queue";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection(Address.parseAddresses(CONN_STRING));
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EX, BuiltinExchangeType.DIRECT);

        boolean durable = true;
        channel.queueDeclare(Q, durable, false, false, null);
        channel.queueBind(Q, EX, Q);

        for (int i = 0; i < 10; i++) {
            String message = "Hello World! " + i;
            channel.basicPublish(EX, Q, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            log.info("Message sent: " + message);
            Thread.sleep(1000);
        }

        channel.close();
        connection.close();
    }
}
