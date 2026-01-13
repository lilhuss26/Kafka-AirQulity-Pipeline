package org.pipeline.database.Influx;

import io.github.cdimascio.dotenv.Dotenv;
import com.influxdb.v3.client.InfluxDBClient;

public class InfluxConfig {
    private Dotenv dotenv = Dotenv.load();
    private final char[] authToken;
    private InfluxDBClient client;

    public InfluxConfig() {
        this.authToken = dotenv.get("INFLUXDB_TOKEN").toCharArray();
        String hostUrl = "https://us-east-1-1.aws.cloud2.influxdata.com";
        try{
            this.client = InfluxDBClient.getInstance(hostUrl, authToken, "Air-Qulity");
        }catch(Exception e){
            System.err.println("✗ Failed to connect to InfluxDB: " + e.getMessage());
        }
    }

    public InfluxDBClient getClient() {
        if (client == null) {
            throw new IllegalStateException("Database client not initialized");
        }
        return client;
    }

}

