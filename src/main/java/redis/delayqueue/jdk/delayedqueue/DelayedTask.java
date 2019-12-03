package redis.delayqueue.jdk.delayedqueue;

import java.util.Calendar;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 定义放在延时队列中的对象，需要实现Delayed接口
 */
public class DelayedTask implements Delayed {

    private long _expireInSecond = 0;

    public DelayedTask(int delaySecond) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, delaySecond);
        System.out.println("expire time: " + cal.getTime());
        this._expireInSecond = (cal.getTimeInMillis());
    }

    public long getDelay(TimeUnit unit) {
        Calendar calendar = Calendar.getInstance();

        //距离消息的expire_time(过期时间)的剩余时间
        return _expireInSecond - calendar.getTimeInMillis();
    }

    //DelayQueue在jdk中是使用优先队列来实现的，排序的依据就是
    //expire_time的大小，expire_time越小，则排序越靠前
    public int compareTo(Delayed o) {
        long d = (getDelay(TimeUnit.SECONDS) - o.getDelay(TimeUnit.SECONDS));
        return (d == 0 ? 0 : (d < 0 ? -1 : 1));
    }
}
