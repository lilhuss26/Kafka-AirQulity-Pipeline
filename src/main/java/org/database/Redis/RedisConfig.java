package org.database.Redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisConfig {
    private static JedisPool pool;

    public static JedisPool getPool() {
        if (pool == null) {
            pool = new JedisPool("localhost", 6379);
        }
        return pool;
    }

    public static Jedis getClient() {
        return getPool().getResource();
    }
}
