package shuaicj.example.rabbitmq.demo05.topic;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Demo05Producer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String EX = "demo05-exchange";
    private static final String K1 = "demo05.hello.me";
    private static final String K2 = "demo05.big.world";
    private static final String K3 = "demo05.hello.world";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection(Address.parseAddresses(CONN_STRING));
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EX, BuiltinExchangeType.TOPIC);

        for (int i = 0; i < 10; i++) {
            String message = "Hello World! " + i;
            if (i % 3 == 0) {
                channel.basicPublish(EX, K1, null, message.getBytes());
                log.info(K1 + " Message sent: " + message);
            } else if (i % 3 == 1) {
                channel.basicPublish(EX, K2, null, message.getBytes());
                log.info(K2 + " Message sent: " + message);
            } else {
                channel.basicPublish(EX, K3, null, message.getBytes());
                log.info(K3 + " Message sent: " + message);
            }
            Thread.sleep(1000);
        }

        channel.close();
        connection.close();
    }
}
