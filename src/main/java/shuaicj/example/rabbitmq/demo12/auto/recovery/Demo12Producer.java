package shuaicj.example.rabbitmq.demo12.auto.recovery;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import lombok.extern.slf4j.Slf4j;

/**
 * See http://www.rabbitmq.com/api-guide.html#recovery.
 */
@Slf4j
public class Demo12Producer {

    private static final String CONN_STRING = "127.0.0.1";

    private static final String Q = "demo12-queue";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        Address[] addresses = Address.parseAddresses(CONN_STRING);
        Connection connection = RetryUtil.retry("new connection", () -> factory.newConnection(addresses));
        Channel channel = RetryUtil.retry("new channel", () -> connection.createChannel());

        boolean durable = true;
        RetryUtil.retry("queue declare", () -> channel.queueDeclare(Q, durable, false, false, null));

        RetryUtil.retry("confirm select", () -> channel.confirmSelect());

        for (int i = 0; i < 100000; i++) {
            String message = "Hello World! " + i;
            RetryUtil.retryForever("publish",
                    () -> channel.basicPublish("", Q, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes()));
            log.info("Message sent: " + message);
            sleep();
            boolean confirmOk = RetryUtil.retryForever("wait confirm", () -> channel.waitForConfirms());
            log.info("Confirm ok: " + confirmOk);
            if (!confirmOk) {
                log.error("confirm not ok, this should not happen");
            }
            sleep();
        }

        RetryUtil.retry("channel close", () -> channel.close());
        RetryUtil.retry("connection close", () -> connection.close());
    }

    private static void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
