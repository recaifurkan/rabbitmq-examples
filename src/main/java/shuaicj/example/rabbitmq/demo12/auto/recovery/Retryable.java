package shuaicj.example.rabbitmq.demo12.auto.recovery;

public interface Retryable {

    void run() throws Exception;
}
