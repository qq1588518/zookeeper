package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * 异步创建节点
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/3/13 21:29
 */
@Slf4j
public class AsyncCreateTest {

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
        zooKeeper.create(
                "/master",          // 节点路径
                serverId.getBytes(),       // 节点数据
                OPEN_ACL_UNSAFE,          // ACL策略
                CreateMode.EPHEMERAL,      // 节点类型：临时节点
                (rc, path, ctx, name) -> {  // AsyncCallback 实例
                    // rc = OK, path = /master, ctx = 2018-03-13T21:48:28.956, name = /master
                    log.info("rc = {}, path = {}, ctx = {}, name = {}",
                            KeeperException.Code.get(rc), path, ctx, name);
                },
                LocalDateTime.now()        // ctx，任意Object对象
        );

        // 获取数据 ( 异步 )
        zooKeeper.getData(
                "/master",
                false,
                (rc, path, ctx, data, stat) -> {
                    // rc = OK, path = /master, ctx = null, data = 31dc72bb, stat = 34359738393,34359738393,1520945530418,1520945530418,0,0,0,171734137076645892,8,0,34359738393
                    log.info("rc = {}, path = {}, ctx = {}, data = {}, stat = {}",
                            KeeperException.Code.get(rc), path, ctx, new String(data), stat);
                },
                null);

        // 等待异步回调执行完成
        TimeUnit.SECONDS.sleep(3L);

        zooKeeper.close();

    }

}
