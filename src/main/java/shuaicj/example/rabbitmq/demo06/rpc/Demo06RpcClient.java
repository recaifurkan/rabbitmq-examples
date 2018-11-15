package shuaicj.example.rabbitmq.demo06.rpc;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
public class Demo06RpcClient {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String Q_REQ = "demo06-queue-req";
    private static final String Q_RSP = "demo06-queue-rsp";

    private static Connection connection;
    private static Channel channel;

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        init();

        String message = "Hello World!";
        String rsp = rpcCapitalize(message);
        log.info("RPC result: " + rsp);

        close();
    }

    private static void init() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        connection = factory.newConnection(Address.parseAddresses(CONN_STRING));
        channel = connection.createChannel();

        channel.queueDeclare(Q_REQ, true, false, false, null);
        channel.queueDeclare(Q_RSP, true, false, false, null);
    }

    private static void close() throws IOException, TimeoutException {
        channel.close();
        connection.close();
    }

    private static String rpcCapitalize(String message) throws IOException, InterruptedException {
        String corrId = UUID.randomUUID().toString();

        channel.basicPublish(
                "",
                Q_REQ,
                new AMQP.BasicProperties.Builder().correlationId(corrId).replyTo(Q_RSP).build(),
                message.getBytes());

        log.info(corrId + " Message sent: " + message);

        BlockingQueue<String> result = new ArrayBlockingQueue<>(1);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) {
                if (properties.getCorrelationId().equals(corrId)) {
                    String message = new String(body);
                    log.info(corrId + " Message received: " + message);
                    result.offer(message);
                }
            }
        };

        channel.basicConsume(Q_RSP, true, consumer);

        return result.take();
    }
}
