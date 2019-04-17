package com.matas.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
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
public class ZkTest {
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final String ZK_PATH = "/zktest";

    /**
     *实验表明，多线程操作同一个node的数据，会有并发问题
     *
     * @param args
     * @return
     * @author matas
     * @date 2019/4/17 11:38
     */
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

        //3.多线程测试往同一个节点并发写date
        IntStream.range(0, threadCount)
                .forEach(i -> {
                    new Thread(() -> {
                        //4.获取节点的值，加1，然后重新设置进去
                        try {
                            byte[] bytes = client.getData().forPath(ZK_PATH);
                            int val = ByteBuffer.wrap(bytes).getInt();
                            log.warn("线程{}获取值为=>{}", i,val);
                            val++;
                            log.warn("线程{}设置值为=>{}", i,val);
                            client.setData().forPath(ZK_PATH, ByteBuffer.allocate(4).putInt(val).array());
                        } catch (Exception e) {
                            e.printStackTrace();
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
