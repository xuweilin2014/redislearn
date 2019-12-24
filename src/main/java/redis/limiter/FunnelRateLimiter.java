package redis.limiter;

import com.sun.glass.ui.View;

import java.rmi.dgc.Lease;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class FunnelRateLimiter {

    static class Funnel{
        private int capacity;
        private float leakingRate;
        private float leftQuota;
        private long lastLeakingTime;

        Funnel(int capacity, float leakingRate){
            //漏斗容量
            this.capacity = capacity;
            //漏斗漏水的速率
            this.leakingRate = leakingRate;
            //漏斗的剩余空间
            this.leftQuota =capacity;
            //上一次漏水的时间
            this.lastLeakingTime = System.currentTimeMillis();
        }

        public void makeSpace(){
            long delta = System.currentTimeMillis();
            //距离上一次漏水过去了多久
            long interval = delta - lastLeakingTime;
            //在距离上一次漏水的这段时间里面，总共漏了多少水
            double leakedQuota = interval * leakingRate;
            // 间隔时间太长，整数数字过大溢出
            if (leakedQuota < 0){
                leftQuota = capacity;
                lastLeakingTime = delta;
                return;
            }
            // 腾出空间太小，最小单位是1
            if (leakedQuota < 1){
                return;
            }
            //增加剩余空间
            leftQuota += leakedQuota;
            //记录漏水的时间
            lastLeakingTime = delta;
            //剩余的容量不得多于漏斗的总容量
            if (leftQuota > capacity){
                leftQuota = capacity;
            }
        }

        public boolean watering(int quota){
            makeSpace();
            //判断剩余空间是否足够
            if (leftQuota >= quota){
                leftQuota -= quota;
                return true;
            }
            return false;
        }
    }

    private Map<String, Funnel> funnels = new HashMap<>();

    public boolean isActionAllowed(String userId, String actionType, int capacity, float leakingRate){
        String key = String.format("hist:%s acts %s", userId, actionType);
        Funnel funnel = funnels.get(key);
        if (funnel == null){
            funnel = new Funnel(capacity, leakingRate);
            funnels.put(key, funnel);
        }
        // 需要1个quota
        return funnel.watering(1);
    }

}
