package redis.hyperlog;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 对于HyperLogLog算法的一个简单实现，和前面的Easy版本相比，
 * 这个版本使用了多轮或者说多个桶，每一轮分别计算出一个maxbits，
 * 最后根据这些maxbits进行一个调和平均计算出较为准确地maxbits值
 * 从而对n的个数进行估算
 */
public class HyperLogLogComplex{

    static class BitKeeper{
        int maxbits = 0;

        public void random(long val){
            int zeroBits = lowZeros(val);
            if (zeroBits > maxbits){
                maxbits = zeroBits;
            }
        }

        private int lowZeros(long val) {
            int i = 1;
            for (; i < 32; i++){
                if (val >> i << i != val){
                    break;
                }
            }
            return i - 1;
        }
    }

    static class Experiment{
        private BitKeeper[] bitKeepers;
        private int buckets;
        private int n;

        //bucket的值默认为2048个
        Experiment(int n){
            this(n, 2048);
        }

        Experiment(int n, int buckets) {
            this.n = n;
            this.buckets = buckets;
            bitKeepers = new BitKeeper[buckets];
            for (int i = 0; i < buckets; i++){
                bitKeepers[i] = new BitKeeper();
            }
        }

        public void exp(){
            for (int i = 0; i < n; i++){
                long val = ThreadLocalRandom.current().nextLong(1L << 32);
                //截取val中，第17~27位(第1位计为1，而不是0)总共11位来判断此val属于哪一个bucket
                BitKeeper bitKeeper = bitKeepers[((int) (val & 0x7ff0000) >> 16) % bitKeepers.length];
                bitKeeper.random(val);
            }
        }

        //对每一个bitKeeper中的maxbits值使用调和平均来计算出最终的maxbits
        //并且依据此maxbits来计算出估算的n值
        public double estimate(){
            double avgBits = 0.0;
            for (int i = 0; i < buckets; i++){
                if (bitKeepers[i].maxbits != 0){
                    avgBits += 1.0 / bitKeepers[i].maxbits;
                }
            }
            avgBits = buckets / avgBits;
            return Math.pow(2, avgBits) * buckets;
        }
    }

    public static void main(String[] args) {
        for (int i = 100000; i <= 1000000; i += 100000) {
            Experiment exp = new Experiment(i);
            exp.exp();
            double est = exp.estimate();

            //Math.abs(est - i) / i表示估算的值est和实际值i之间的误差
            System.out.printf("%d %.2f %.2f\n", i, est, Math.abs(est - i) / i);
        }
    }

}
