package com.wb.drools.spark.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author wenbao
 * @version 1.0.0
 * @email wenbao@yijiupi.com
 * @Description 获取redis的连接
 * @createTime 14:15 2020/5/8
 */
public class RedisUtil {

    private static transient JedisSentinelPool jedisSentinelPool;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(60);
        jedisPoolConfig.setMaxIdle(60);
        jedisPoolConfig.setMinIdle(60);
        Set<String> sentinels = new HashSet<String>(Arrays.asList(("XXX:26379,XXX:26379,XXX:26379".split(","))));
        jedisSentinelPool =
                new JedisSentinelPool("mymaster", sentinels, jedisPoolConfig, 1200000, null, 5);
    }

    public static Jedis getJedis() {
        return jedisSentinelPool.getResource();
    }
}
