package shuaicj.example.rabbitmq.demo01.hello.world;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Demo01Producer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String Q = "demo01-queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection(Address.parseAddresses(CONN_STRING));
        Channel channel = connection.createChannel();

        channel.queueDeclare(Q, false, false, false, null);

        String message = "Hello World!";
        channel.basicPublish("", Q, null, message.getBytes());
        log.info("Message sent: " + message);

        channel.close();
        connection.close();
    }
}
