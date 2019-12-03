package redis.delayqueue;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.lang.reflect.Type;
import java.util.Set;
import java.util.UUID;

/**
 * 使用Redis来实现一个延时队列，使用Redis的zset数据结构来实现
 * 在本次实现中，有一个生产者producer，向延时队列中添加消息；同时有两个消费者consumer，从延时队列中取出数据
 * @param <T>
 */
public class RedisDelayingQueue<T> {

    //redis的java客户端jedis不是线程安全的，所以通过JedisPool来给每一个线程
    //分配一个jedis，向redis服务器发送命令
    private static JedisPool jedisPool = new JedisPool();
    private String queueName;

    // fastjson 序列化对象中存在 generic 类型时，需要使用 TypeReference
    private Type taskType = new TypeReference<Task<T>>(){}.getType();

    public RedisDelayingQueue(String queueName){
        this.queueName = queueName;
    }

    public void addTask(Task<T> task, Jedis jedis){
        String strTask = JSON.toJSONString(task);
        //加上5s，表示这个消息至少过5s之后再做处理
        jedis.zadd(queueName, System.currentTimeMillis() + 5000, strTask);
    }

    public void loop(Jedis jedis){
        while (!Thread.interrupted()){
            Set<String> tasks = jedis.zrangeByScore(queueName, 0, System.currentTimeMillis(), 0, 1);
            if (tasks.isEmpty()){
                try {
                    //在线程睡眠的时候，如果发生了中断，则退出循环
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
                continue;
            }
            String task = tasks.iterator().next();

            // zrem 方法是多线程多进程争抢任务的关键，它的返回值决定了当前实例有没有抢到任务，
            // 因为 loop 方法可能会被多个线程、多个进程调用，同一个任务可能会被多个进程线程抢到，
            // 通过 zrem 来决定唯一的属主。
            if (jedis.zrem(queueName, task) > 0){
                handle_task(task);
            }
        }
    }

    private void handle_task(String task) {
        Task<T> newTask = JSON.parseObject(task, taskType);
        System.out.println(newTask.getData());
    }

    public static void main(String[] args){
        final RedisDelayingQueue<String> redisDelayingQueue =
                new RedisDelayingQueue<String>( "q-demo");

        //消息生产者
        Thread producer = new Thread(new Runnable() {
            public void run() {
                Jedis jedis = jedisPool.getResource();
                try{
                    for (int i = 0; i < 10; i++){
                        String uuid = UUID.randomUUID().toString().substring(0,6);
                        redisDelayingQueue.addTask(new Task<String>(uuid, "task-" + uuid), jedis);
                    }
                }finally {
                    jedis.close();
                }

            }
        }, "producer");

        //消息消费者1
        Thread consumer1 = new Thread(new Runnable() {
            public void run() {
                Jedis jedis = jedisPool.getResource();
                try{
                    redisDelayingQueue.loop(jedis);
                }finally {
                    jedis.close();
                }
            }
        }, "consumer1");

        //消息消费者2
        Thread consumer2 = new Thread(new Runnable() {
            public void run() {
                Jedis jedis = jedisPool.getResource();
                try{
                    redisDelayingQueue.loop(jedis);
                }finally {
                    jedis.close();
                }
            }
        }, "consumer2");

        producer.start();
        consumer1.start();
        consumer2.start();

        try {
            producer.join();
            Thread.sleep(6000);
            consumer1.interrupt();
            consumer2.interrupt();
            consumer1.join();
            consumer2.join();
        } catch (InterruptedException e) {
        }finally {
            redisDelayingQueue.closeJedis();
        }
    }

    private void closeJedis(){
        jedisPool.close();
        System.out.println("关闭Redis连接池");
    }

}
