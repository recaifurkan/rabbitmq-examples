package shuaicj.example.rabbitmq.demo04.routing;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Demo04Producer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String EX = "demo04-exchange";
    private static final String K1 = "demo04.key1";
    private static final String K2 = "demo04.key2";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection(Address.parseAddresses(CONN_STRING));
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EX, BuiltinExchangeType.DIRECT);

        for (int i = 0; i < 10; i++) {
            String message = "Hello World! " + i;
            if (i % 2 == 0) {
                channel.basicPublish(EX, K1, null, message.getBytes());
                log.info(K1 + " Message sent: " + message);
            } else {
                channel.basicPublish(EX, K2, null, message.getBytes());
                log.info(K2 + " Message sent: " + message);
            }
            Thread.sleep(1000);
        }

        channel.close();
        connection.close();
    }
}
