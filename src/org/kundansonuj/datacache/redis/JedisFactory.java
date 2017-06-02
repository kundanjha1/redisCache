package org.kundansonuj.datacache.redis;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;


public class JedisFactory {
    public Jedis createJedisClient(String url, Integer port) {
        if(StringUtils.isEmpty(url)) {
            return null;
        }
        return port != null ? new Jedis(url, port) : new Jedis(url);
    }
}
