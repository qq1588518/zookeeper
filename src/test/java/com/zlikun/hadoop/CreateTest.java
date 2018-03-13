package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * 创建节点
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/3/13 21:29
 */
@Slf4j
public class CreateTest {

    @Test
    public void test() throws IOException, InterruptedException, KeeperException {

        // ZooKeeper 连接字符串
        final String connectionString = AppConstants.CONNECTIONS;
        // ZooKeeper 会话等待超时设置，单位：毫秒，表示指定时间内ZK与客户端无法通信则ZK会终止会话
        final int sessionTimeout = 5000;
        // Zookeeper 监视器
        // 测试可知：如果ZK服务端未启动，客户端不会执行Watcher
        final Watcher watcher = event -> log.info("event => {}", event);

        // 构造一个ZooKeeper句柄
        ZooKeeper zooKeeper = new ZooKeeper(connectionString, sessionTimeout, watcher);

        // 创建节点
        String serverId = Integer.toHexString(new Random().nextInt());
        String node = zooKeeper.create(
                "/master",      // 节点路径
                serverId.getBytes(),    // 节点数据
                OPEN_ACL_UNSAFE,        // ACL策略
                CreateMode.EPHEMERAL    // 节点类型：临时节点
        );
        log.info("node = {}", node);

        // 获取节点数据
        byte [] data = zooKeeper.getData("/master", false, new Stat());
        log.info("data = {}", new String(data));

        zooKeeper.close();

    }

}
