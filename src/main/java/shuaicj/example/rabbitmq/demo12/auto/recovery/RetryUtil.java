package shuaicj.example.rabbitmq.demo12.auto.recovery;

import java.util.concurrent.Callable;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetryUtil {

    public static final long DEFAULT_RETRIES = 3L;
    public static final long DEFAULT_INTERVAL = 5000L; // 5000 milliseconds

    public static <T> T retryForever(String message, Callable<T> callable) throws RetryException {
        return retry(message, callable, Long.MAX_VALUE);
    }

    public static <T> T retryForever(String message, Callable<T> callable, long interval) throws RetryException {
        return retry(message, callable, Long.MAX_VALUE, interval);
    }

    public static void retryForever(String message, Retryable retryable) throws RetryException {
        retry(message, retryable, Long.MAX_VALUE);
    }

    public static void retryForever(String message, Retryable retryable, long interval) throws RetryException {
        retry(message, retryable, Long.MAX_VALUE, interval);
    }

    public static <T> T retry(String message, Callable<T> callable) throws RetryException {
        return retry(message, callable, DEFAULT_RETRIES);
    }

    public static <T> T retry(String message, Callable<T> callable, long maxRetries) throws RetryException {
        return retry(message, callable, maxRetries, DEFAULT_INTERVAL);
    }

    public static void retry(String message, Retryable retryable) throws RetryException {
        retry(message, retryable, DEFAULT_RETRIES);
    }

    public static void retry(String message, Retryable retryable, long maxRetries) throws RetryException {
        retry(message, retryable, maxRetries, DEFAULT_INTERVAL);
    }

    public static <T> T retry(String message, Callable<T> callable, long maxRetries, long interval)
            throws RetryException {
        for (long i = 0; i < maxRetries; i++) {
            try {
                return callable.call();
            } catch (Exception e) {
                log.warn(message + " retry " + i + " failed, " + e.toString());
                if (i == maxRetries - 1) {
                    throw new RetryException(message + " retry " + i + " failed finally", e);
                } else {
                    try {
                        Thread.sleep(interval);
                    } catch (InterruptedException e1) {
                        throw new RetryException(message + " retry " + i + " interrupted", e1);
                    }
                }
            }
        }
        // impossible to be here
        return null;
    }

    public static void retry(String message, Retryable retryable, long maxRetries, long interval)
            throws RetryException {
        for (long i = 0; i < maxRetries; i++) {
            try {
                retryable.run();
                return;
            } catch (Exception e) {
                log.warn(message + " retry " + i + " failed, " + e.toString());
                if (i == maxRetries - 1) {
                    throw new RetryException(message + " retry " + i + " failed finally", e);
                } else {
                    try {
                        Thread.sleep(interval);
                    } catch (InterruptedException e1) {
                        throw new RetryException(message + " retry " + i + " interrupted", e1);
                    }
                }
            }
        }
    }
}
