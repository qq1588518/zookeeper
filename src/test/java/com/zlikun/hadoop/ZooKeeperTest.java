package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * ZooKeeper 句柄构造测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/3/13 20:58
 */
@Slf4j
public class ZooKeeperTest {

    @Test
    public void test() throws IOException, InterruptedException {

        // ZooKeeper 连接字符串
        final String connectionString = AppConstants.CONNECTIONS;
        // ZooKeeper 会话等待超时设置，单位：毫秒，表示指定时间内ZK与客户端无法通信则ZK会终止会话
        final int sessionTimeout = 5000;
        // Zookeeper 监视器
        // 测试可知：如果ZK服务端未启动，客户端不会执行Watcher
        final Watcher watcher = event -> log.info("event => {}", event);

        // 构造一个ZooKeeper句柄
        ZooKeeper zooKeeper = new ZooKeeper(connectionString, sessionTimeout, watcher);

        // 我们连到ZK后，后台会有一个守护线程来这个ZK会话。
        // 该线程为守护线程，即：即使会话处理活跃状态，程序也可以退出
        // 所以这里程序退出前休眠一段时间，以便观察事件发生
        TimeUnit.SECONDS.sleep(7L);
        log.info("program exit .");

        zooKeeper.close();

    }

}
