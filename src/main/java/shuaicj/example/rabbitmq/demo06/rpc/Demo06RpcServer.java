package shuaicj.example.rabbitmq.demo06.rpc;

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
public class Demo06RpcServer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String Q_REQ = "demo06-queue-req";

    public static void main(String[] args) throws IOException, TimeoutException {
        serve();
    }

    private static void serve() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection(Address.parseAddresses(CONN_STRING));
        Channel channel = connection.createChannel();
        channel.queueDeclare(Q_REQ, true, false, false, null);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body);
                String corrId = properties.getCorrelationId();
                log.info(corrId + " Message received: " + message);

                String rspMessage = capitalize(message);

                channel.basicPublish(
                        "",
                        properties.getReplyTo(),
                        new AMQP.BasicProperties.Builder().correlationId(corrId).build(),
                        rspMessage.getBytes());

                log.info(corrId + " Response sent: " + rspMessage);
            }
        };

        channel.basicConsume(Q_REQ, true, consumer);
    }

    private static String capitalize(String message) {
        return message.toUpperCase();
    }
}
