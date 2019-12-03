package redis.delayqueue.jdk.delayedqueue;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.DelayQueue;


public class DelayedQueueTest {
    public static void main(String[] args) throws InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        DelayQueue<DelayedTask> delayQueue = new DelayQueue<DelayedTask>();

        DelayedTask task1 = new DelayedTask(10);
        DelayedTask task2 = new DelayedTask(5);
        DelayedTask task3 = new DelayedTask(15);

        System.out.println();

        delayQueue.offer(task1);
        delayQueue.offer(task2);
        delayQueue.offer(task3);

        System.out.println(sdf.format(new Date()) + " start");

        while (delayQueue.size() != 0){
            DelayedTask task = delayQueue.poll();

            //如果队列中没有任务到expire_time(过期时间)，
            //则取出的task值为null
            if (task != null){
                System.out.println(sdf.format(new Date()));
            }

            Thread.sleep(1000);
        }
    }
}
