package redis.hyperlog;

import java.util.concurrent.ThreadLocalRandom;

/**
 * HyperLogLog的一个简单实现
 */
public class HyperLogLogEasy {
    static class BitKeeper{
        protected int maxbits = 0;

        public void random(){
            long val = ThreadLocalRandom.current().nextLong(1L << 32);
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
        private BitKeeper bitKeeper;
        private int n;

        Experiment(BitKeeper bitKeeper, int n){
            this.bitKeeper = bitKeeper;
            this.n = n;
        }

        public void exp(){
            for (int i = 0; i < n; i++){
                bitKeeper.random();
            }
        }

        public void debug(){
            System.out.printf("%d  %d  %.2f\n", this.n, bitKeeper.maxbits, Math.log(n) / Math.log(2));
        }
    }

    public static void main(String[] args) {
        for (int i = 1000; i < 100000; i+=100){
            Experiment exp = new Experiment(new BitKeeper(), i);
            exp.exp();
            exp.debug();
        }
    }
}
