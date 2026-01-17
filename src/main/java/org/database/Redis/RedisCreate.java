package org.database.Redis;

import redis.clients.jedis.Jedis;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class RedisCreate {
    private static final String LAST_RECORD_KEY = "air-quality:last-record";
    private ObjectMapper objectMapper;

    public RedisCreate() {
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public boolean insertLastRecord(String recordJson) {
        try (Jedis jedis = RedisConfig.getClient()) {
            String result = jedis.set(LAST_RECORD_KEY, recordJson);
            if ("OK".equals(result)) {
                System.out.println("✅ Successfully inserted last record to Redis");
                return true;
            } else {
                System.err.println("❌ Failed to insert record to Redis");
                return false;
            }
        } catch (Exception e) {
            System.err.println("❌ Redis insertion error: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public String getLastRecord() {
        try (Jedis jedis = RedisConfig.getClient()) {
            return jedis.get(LAST_RECORD_KEY);
        } catch (Exception e) {
            System.err.println("❌ Redis retrieval error: " + e.getMessage());
            return null;
        }
    }
}