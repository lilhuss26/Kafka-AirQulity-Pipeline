package org.pipeline.database;

import java.time.Instant;
import java.util.stream.Stream;
import io.github.cdimascio.dotenv.Dotenv;
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.query.QueryOptions;
import com.influxdb.v3.client.query.QueryType;
import com.influxdb.v3.client.write.WriteOptions;

public class Config {
    private Dotenv dotenv = Dotenv.load();
    private final char[] authToken;
    private InfluxDBClient client;

    public Config() {
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

