package shuaicj.example.rabbitmq.demo12.auto.recovery;

@SuppressWarnings("serial")
public class RetryException extends RuntimeException {

    public RetryException(String message, Throwable cause) {
        super(message, cause);
    }
}
