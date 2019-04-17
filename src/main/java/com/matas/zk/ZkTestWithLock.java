package com.matas.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

/**
 * @author matas
 * @date 2019/4/17 10:53
 * @email mataszhang@163.com
 */
@Slf4j
public class ZkTestWithLock {
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final String ZK_PATH = "/zktest";

    public static void main(String[] args) throws Exception {
        // 1.连接zk
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                ZK_ADDRESS,
                new RetryNTimes(10, 5000)
        );
        client.start();

        //2.创建node
        if (client.checkExists().forPath(ZK_PATH) != null) {
            log.warn("{}节点已存在，将删除，然后重新创建",ZK_PATH);
            client.delete().forPath(ZK_PATH);
        }

        client.create()
                .creatingParentsIfNeeded()
                .forPath(ZK_PATH, ByteBuffer.allocate(4).putInt(0).array());

        log.warn("根节点下有=>" + client.getChildren().forPath("/"));


        int threadCount = 100;
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);

        //分布式锁
        InterProcessMutex lock = new InterProcessMutex(client,ZK_PATH);

        //3.多线程测试往同一个节点并发写date
        IntStream.range(0, threadCount)
                .forEach(i -> {
                    new Thread(() -> {
                        //4.获取节点的值，加1，然后重新设置进去
                        try {
                            lock.acquire();
                            byte[] bytes = client.getData().forPath(ZK_PATH);
                            int val = ByteBuffer.wrap(bytes).getInt();
                            log.warn("----线程{}获取值为=>{}", i,val);
                            val++;
                            log.warn("++++线程{}设置值为=>{}", i,val);
                            client.setData().forPath(ZK_PATH, ByteBuffer.allocate(4).putInt(val).array());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }finally {
                            try {
                                lock.release();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        countDownLatch.countDown();
                    }).start();
                });

        countDownLatch.await();

        byte[] bytes = client.getData().forPath(ZK_PATH);
        int val = ByteBuffer.wrap(bytes).getInt();
        log.warn("=========={}===========",val);
    }
}
