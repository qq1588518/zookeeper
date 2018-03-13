package com.zlikun.hadoop.watcher;

import com.zlikun.hadoop.AppConstants;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * 自定义Watcher
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017/4/4 16:39
 */
public class AsyncMasterWatcher implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(AsyncMasterWatcher.class) ;

    ZooKeeper zk ;
    String hostPort ;

    String serverId = Integer.toHexString(new Random().nextInt()) ;
    private boolean isLeader;

    public AsyncMasterWatcher(String hostPort) {
        this.hostPort = hostPort;
    }

    public void startZk() throws IOException {
        zk = new ZooKeeper(hostPort ,15000 ,this) ;
    }

    public void stopZk() throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }

    private void checkMaster() {
        zk.getData("/master", false, (i, s, o, bytes, stat) -> {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    checkMaster() ;
                    return ;
                case NONODE:
                    runForMaster();
                    return ;
            }
        },null);
    }

    public void runForMaster() {
        zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, (i, s, o, s1) -> {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    checkMaster() ;
                    return ;
                case OK:
                    isLeader = true ;
                    return ;
                default:
                    isLeader = false ;
            }
            log.info("I'm {}the leader" ,isLeader ? "" : "not ");
        },null);
        isLeader = true ;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        AsyncMasterWatcher m = new AsyncMasterWatcher(AppConstants.CONNECTIONS) ;
        m.startZk();
        m.runForMaster();
        if(m.isLeader) {
            log.info("I'm the leader");
            TimeUnit.SECONDS.sleep(60);
        } else {
            log.info("Someone else is the leader");
        }
        m.stopZk();

    }
}