package test;

import java.util.Random;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        System.out.println(Thread.currentThread().getName() + " 主线程开始运行");

        ThreadTest t1 = new ThreadTest("A");
        ThreadTest t2 = new ThreadTest("B");

        t1.start();
        t2.start();

        t1.join();
        t2.join();
        System.out.println(Thread.currentThread().getName() + " 主线程运行结束");
    }

    static class ThreadTest extends Thread{
        private String name;

        public ThreadTest(String name) {
            super(name);
            this.name = name;
        }

        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName() + " 线程开始运行");
            for (int i = 0; i < 5; i++){
                System.out.println("子线程" + name + "运行：" + i);
                try {
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println(Thread.currentThread().getName() + " 线程运行结束");
        }
    }


}
