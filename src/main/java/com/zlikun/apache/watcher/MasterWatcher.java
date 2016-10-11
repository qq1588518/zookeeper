package com.zlikun.apache.watcher;

import com.zlikun.apache.Consts;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * 自定义Watcher
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017/4/4 16:39
 */
public class MasterWatcher implements Watcher {

    ZooKeeper zk ;

    String serverId = Integer.toHexString(new Random().nextInt()) ;
    private boolean isLeader;

    public void startZk() throws IOException {
        zk = new ZooKeeper(Consts.CONNECTIONS,15000 ,this) ;
    }

    public void stopZk() throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }

    private boolean checkMaster() throws InterruptedException {
        while (true) {
            try {
                byte [] data = zk.getData("/master" ,false ,new Stat());
                isLeader = new String(data).equals(serverId) ;
                return true ;
            } catch (KeeperException.NoNodeException e) {
                return false ;
            } catch (KeeperException.ConnectionLossException e) {
                // 丢失连接，继续尝试 ...
            } catch (KeeperException e) {
                return false ;
            }
        }
    }

    public void runForMaster() throws InterruptedException {
        while (true) {
            try {
                zk.create("/master" ,serverId.getBytes() ,OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL) ;
                isLeader = true ;
                break ;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false ;
                break ;
            } catch (KeeperException.ConnectionLossException e) {

            } catch (KeeperException e) {

            }
            if(checkMaster()) break;
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        MasterWatcher m = new MasterWatcher() ;
        m.startZk();
        m.runForMaster();
        if(m.isLeader) {
            System.out.println("I'm the leader");
            TimeUnit.SECONDS.sleep(60);
        } else {
            System.out.println("Someone else is the leader");
        }
        m.stopZk();

    }
}