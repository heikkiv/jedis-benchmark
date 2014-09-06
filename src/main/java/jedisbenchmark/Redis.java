package jedisbenchmark;

import redis.clients.jedis.*;


public class Redis {

    private JedisPool pool;

    public Redis(String host, int poolSize) {
        destroy();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(poolSize);
        pool = new JedisPool(poolConfig, host);
    }

    public void set(String key, String value) {
        Jedis jedis = pool.getResource();
        try {
            jedis.set(key, value);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public String get(String key) {
        Jedis jedis = pool.getResource();
        try {
            return jedis.get(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public void destroy() {
        if(pool != null) {
            pool.destroy();
        }
    }

}
