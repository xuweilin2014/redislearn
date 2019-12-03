package redis.lock.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.ZKUtil;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 下面的代码将创建 clientNums 个线程来模拟分布式系统中的节点
 * 系统将通过 InterProcessMutex 来控制对资源的同步使用
 */
public class InterProcessMutexTest {

    //创建的客户端的数量
    private static final int clientNums = 3;

    private static CountDownLatch countDownLatch = new CountDownLatch(clientNums);

    //模拟的临界资源
    private static FakeLimitResource limitResource = new FakeLimitResource();

    public static final String path = "/testZK/sharedreentrantlock";

    /**
     * 此方法使用可重入的InterProcessMutex锁或者是
     * 不可重入的InterProcessSemaphoreMutex锁
     * @throws InterruptedException
     */
    public static void reentrantOrNotLock () throws InterruptedException {
        for (int i = 0; i < clientNums; i++){
            final String clientName = "#client" + i;

            new Thread(new Runnable() {
                public void run() {
                    //创建一个与Zookeeper相连的客户端
                    CuratorFramework client = CuratorFrameworkFactory.builder()
                            .connectString("localhost:2181")
                            .retryPolicy(new ExponentialBackoffRetry(1000,3))
                            .build();
                    client.start();
                    Random random = new Random();

                    try{
                        //在path指定的路径下创建可重入锁
                        InterProcessMutex lock = new InterProcessMutex(client, path);

                        //在path指定的路径下创建不可重入锁
                        //InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(client, path);

                        for (int j = 0; j < 2; j++){
                            try{
                                //第一次获取锁
                                if (!lock.acquire(10, TimeUnit.SECONDS)){
                                    throw new IllegalStateException(j + ":  " + clientName + "不能得到互斥锁");
                                }
                                System.out.println(j + ":  " + clientName + "得到互斥锁");

                                limitResource.useResource();
                                //第二次获取锁
                                if (!lock.acquire(10, TimeUnit.SECONDS)){
                                    throw new IllegalStateException(j + ":  " + clientName + "不能再次得到互斥锁");
                                }
                                System.out.println(j + ":  " + clientName + "再次得到互斥锁");
                                System.out.println(j + ":  " + clientName + "释放互斥锁");
                            }catch (Exception e){
                                System.out.println(e.getMessage());
                            }finally {
                                //获取了几次锁就得释放几次锁
                                if (lock.isAcquiredInThisProcess()){
                                    lock.release();
                                    if (lock.isAcquiredInThisProcess()){
                                        lock.release();
                                    }
                                }
                            }
                            Thread.sleep(random.nextInt(100));
                        }

                    } catch (Throwable e) {
                        System.out.println(e.getMessage());
                    }finally {
                        CloseableUtils.closeQuietly(client);
                        System.out.println(clientName + "客户端已经关闭");
                        countDownLatch.countDown();
                    }
                }
            }, clientName).start();

        }
        //等待所有clientNums个线程全部结束之后Main线程才结束
        countDownLatch.await();
        System.out.println("结束");
    }

    /**
     * 此方法使用Curator中的读写锁
     */
    public static void readAndWriteLock() throws InterruptedException {
        for (int i = 0; i < clientNums; i++){
            final String clientName = "#client" + i;
            new Thread(new Runnable() {
                public void run() {
                    //创建一个与Zookeeper相连的客户端
                    CuratorFramework client = CuratorFrameworkFactory.builder()
                            .connectString("localhost:2181")
                            .retryPolicy(new ExponentialBackoffRetry(1000,3))
                            .build();
                    client.start();

                    InterProcessReadWriteLock readWriteLock = new InterProcessReadWriteLock(client, path);
                    InterProcessMutex readLock = readWriteLock.readLock();
                    InterProcessMutex writeLock = readWriteLock.writeLock();

                    try{
                        if (!writeLock.acquire(1, TimeUnit.SECONDS)){
                            throw new IllegalStateException(clientName + " 不能得到写锁");
                        }
                        System.out.println(clientName + " 已经得到写锁");

                        if (!readLock.acquire(3, TimeUnit.SECONDS)){
                            throw new IllegalStateException(clientName + " 不能得到读锁");
                        }
                        System.out.println(clientName + " 可以得到读锁");


                        limitResource.useResource();
                    }catch (Exception ex){
                        System.out.println(ex.getMessage());
                    }finally {
                        if (readLock.isAcquiredInThisProcess()){
                            try {
                                readLock.release();
                            } catch (Exception e) {
                                System.out.println("读锁释放出现异常");
                            }
                            System.out.println(clientName + " 释放读锁");
                        }

                        if (writeLock.isAcquiredInThisProcess()){
                            try {
                                writeLock.release();
                            } catch (Exception e) {
                                System.out.println("写锁释放出现异常");
                            }
                            System.out.println(clientName + " 释放写锁");
                        }

                        CloseableUtils.closeQuietly(client);
                        System.out.println(clientName + " 关闭");
                        countDownLatch.countDown();
                    }
                }
            }).start();
        }

        countDownLatch.await();
        System.out.println("结束");
    }

    public static void multiSharedLock() throws Exception {
        String lockPath1 = "/testZK/MSLock1";
        String lockPath2 = "/testZK/MSLock2";

        //创建一个与Zookeeper相连的客户端
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                .build();
        client.start();

        //可重入锁
        InterProcessMutex lock1 = new InterProcessMutex(client, lockPath1);
        //不可重入锁
        InterProcessSemaphoreMutex lock2 = new InterProcessSemaphoreMutex(client, lockPath2);
        //创建一个组锁
        InterProcessMultiLock multiLock = new InterProcessMultiLock(Arrays.asList(lock1,lock2));

        try{
            if (!multiLock.acquire(5, TimeUnit.SECONDS)){
                throw new IllegalStateException(" 不能得到组锁");
            }

            System.out.println("已经获取到组锁");
            System.out.println("是否有第一个锁：" + lock1.isAcquiredInThisProcess());
            System.out.println("是否有第二个锁：" + lock2.isAcquiredInThisProcess());

            limitResource.useResource();

            System.out.println("释放组锁");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }finally {
            multiLock.release();
        }

        System.out.println("是否有第一个锁：" + lock1.isAcquiredInThisProcess());
        System.out.println("是否有第二个锁：" + lock2.isAcquiredInThisProcess());
        client.close();
        System.out.println("结束");
    }

    public static void main(String[] args) throws Exception {
        multiSharedLock();
    }

    static class FakeLimitResource {

        private AtomicBoolean inUse = new AtomicBoolean(false);

        public void useResource() throws IllegalAccessException {
            if (!inUse.compareAndSet(false, true)){
                throw new IllegalAccessException("can only be used by one client at the same time");
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                inUse.compareAndSet(true, false);
            }
        }
    }
}
