package name.nicholasgribanov.kafka;

import name.nicholasgribanov.kafka.threads.ConsumerRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "java-group-threads";
        CountDownLatch latch = new CountDownLatch(1);

        log.info("Creating consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(latch, bootstrapServer, groupId, topic);

        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");
        }));



        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupted", e);
        } finally {
            log.info("Application is closing");
        }

    }
}
