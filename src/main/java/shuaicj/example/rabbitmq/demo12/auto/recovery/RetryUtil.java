package shuaicj.example.rabbitmq.demo12.auto.recovery;

import java.util.concurrent.Callable;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetryUtil {

    public static final long DEFAULT_RETRIES = 3L;
    public static final long DEFAULT_DELAY = 5000L; // the unit of delay is millisecond

    public static <T> T retryForever(String message, Callable<T> callable) throws RetryException {
        return retry(message, callable, Long.MAX_VALUE);
    }

    public static <T> T retryForever(String message, Callable<T> callable, long delayMillis) throws RetryException {
        return retry(message, callable, Long.MAX_VALUE, delayMillis);
    }

    public static void retryForever(String message, RetryRunnable runnable) throws RetryException {
        retry(message, runnable, Long.MAX_VALUE);
    }

    public static void retryForever(String message, RetryRunnable runnable, long delayMillis) throws RetryException {
        retry(message, runnable, Long.MAX_VALUE, delayMillis);
    }

    public static <T> T retry(String message, Callable<T> callable) throws RetryException {
        return retry(message, callable, DEFAULT_RETRIES);
    }

    public static <T> T retry(String message, Callable<T> callable, long maxRetries) throws RetryException {
        return retry(message, callable, maxRetries, DEFAULT_DELAY);
    }

    public static void retry(String message, RetryRunnable runnable) throws RetryException {
        retry(message, runnable, DEFAULT_RETRIES);
    }

    public static void retry(String message, RetryRunnable runnable, long maxRetries) throws RetryException {
        retry(message, runnable, maxRetries, DEFAULT_DELAY);
    }

    public static <T> T retry(String message, Callable<T> callable, long maxRetries, long delayMillis)
            throws RetryException {
        for (long i = 0; i < maxRetries; i++) {
            try {
                return callable.call();
            } catch (Throwable e) {
                log.error(message + " retry " + i + " failed, " + e.toString());
                if (i == maxRetries - 1) {
                    throw new RetryException(message + " retry " + i + " failed finally", e);
                } else {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e1) {
                        throw new RetryException(message + " retry " + i + " interrupted", e1);
                    }
                }
            }
        }
        // impossible to be here
        return null;
    }

    public static void retry(String message, RetryRunnable runnable, long maxRetries, long delayMillis)
            throws RetryException {
        for (long i = 0; i < maxRetries; i++) {
            try {
                runnable.run();
                return;
            } catch (Throwable e) {
                log.error(message + " retry " + i + " failed, " + e.toString());
                if (i == maxRetries - 1) {
                    throw new RetryException(message + " retry " + i + " failed finally", e);
                } else {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e1) {
                        throw new RetryException(message + " retry " + i + " interrupted", e1);
                    }
                }
            }
        }
    }
}
