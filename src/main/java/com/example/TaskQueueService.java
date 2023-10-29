package com.example;

import org.apache.commons.configuration2.EnvironmentConfiguration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TaskQueueService {

    private JedisPool jedisPool;

    TaskQueueService() {
        EnvironmentConfiguration config = new EnvironmentConfiguration();
        String redisHost = config.getString("REDIS_HOST");
        redisHost = redisHost == null ? "localhost" : redisHost;
        jedisPool = new JedisPool(redisHost, 6379);
    }

    String getTask(String queueName) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.blpop(0, queueName).get(1);
        }
    }

    void sendTask(String queueName, String message) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.lpush(queueName, message);
        }
    }
}
