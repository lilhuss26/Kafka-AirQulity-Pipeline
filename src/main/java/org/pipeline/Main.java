package org.pipeline;

import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) {
        System.out.println("🚀 Starting Air Quality Pipeline...");

        LocationRecordProducer producer = new LocationRecordProducer();
        InfluxConsumer consumer = new InfluxConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🛑 Shutting down pipeline...");
            producer.shutdown();
            System.out.println("✅ Pipeline stopped gracefully");
        }));

        producer.startPeriodicFetching();
        consumer.startConsuming();

        System.out.println("✅ Pipeline is running. Press Ctrl+C to stop.");

        CountDownLatch latch = new CountDownLatch(1);
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Main thread interrupted");
        }
    }
}