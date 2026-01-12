package org.pipeline;

public class Main {
    public static void main(String[] args) {
        System.out.println("🚀 Starting Air Quality Pipeline...");
        LocationRecordProducer producer = new LocationRecordProducer();
        InfluxConsumer consumer = new InfluxConsumer();
        producer.sendRecords();
        consumer.startConsuming();
    }
}