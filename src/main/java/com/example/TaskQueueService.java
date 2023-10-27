package com.example;

import org.apache.commons.configuration2.EnvironmentConfiguration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TaskQueueService {

    private JedisPool jedisPool;

    TaskQueueService() {
        EnvironmentConfiguration config = new EnvironmentConfiguration();
        String redisHost;
        try {
            redisHost = config.getString("REDIS_HOST");
        } catch (Exception e) {
            redisHost = "localhost";
        }
        jedisPool = new JedisPool(redisHost, 6379);
    }

    String getTask() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.blpop(0, "convert-queue").get(1);
        }
    }
}
