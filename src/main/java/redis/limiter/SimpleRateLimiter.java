package redis.limiter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import java.util.Random;
import java.util.UUID;

public class SimpleRateLimiter {
    private Jedis jedis;

    SimpleRateLimiter(Jedis jedis){
        this.jedis = jedis;
    }

    public boolean isActionAllowed(String userId, String actionType, int period, int maxCount){
        String key = String.format("hist:%s acts %s",userId,actionType);
        long nowTime = System.currentTimeMillis();

        //一次发送多个命令让redis服务器执行
        Pipeline pipeline = jedis.pipelined();

        //开启一次事务
        pipeline.multi();

        //使用UUID是因为程序执行的太快，所以会使得生成的时间戳 nowTime 相同，使得放入zset中的value值相同，
        //而zset会除去重复的值，所以最后生成的true的个数大于5个
        //因此，在nowTime之后加上一个随机值，保证元素的value值不相同
        String randomId = UUID.randomUUID().toString().substring(0,10);

        //ZADD key score member [[score member] [score member] ...]
        //记录行为，value和score都是毫秒时间戳
        //每一次的请求都是有效数据，直接插入到zset中，作为下一次请求执行的标准
        pipeline.zadd(key, nowTime, nowTime + "" + randomId);

        //ZREMRANGEBYRANK key start stop，移除有序集 key 中，score在指定排名(rank)区间内的所有成员
        //区间分别以下标参数 start 和 stop 指出，包含 start 和 stop 在内，下标参数start和stop都以0为底
        pipeline.zremrangeByScore(key, 0, nowTime - period * 1000);

        //返回有序集合key中元素的数量
        Response<Long> num = pipeline.zcard(key);

        //设置 zset 过期时间，避免冷用户持续占用内存
        //过期时间应该等于时间窗口的长度，再多宽限 1s
        pipeline.expire(key, period +1);
        pipeline.exec();
        pipeline.close();
        return num.get() <= maxCount;
    }

    public static void main(String[] args) {
        Jedis jedis = new Jedis();
        SimpleRateLimiter limiter = new SimpleRateLimiter(jedis);
        for (int i = 0; i < 10; i++){
            System.out.println(i + ": " + limiter.isActionAllowed("傅友德", "点赞", 60, 7));
        }
    }
}
