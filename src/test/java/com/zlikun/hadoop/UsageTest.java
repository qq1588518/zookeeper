package com.zlikun.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

@Slf4j
public class UsageTest {

    // ZooKeeper 会话超时设置，单位：毫秒
    final int sessionTimeout = 3000;

    // ZooKeeper 观察者，监听所有被触发的事件
    final Watcher watcher = event -> log.info("已经触发了{}事件！", event.getType());

    ZooKeeper zk;
    String root = "/zlikun";    // 根节点

    @Before
    public void init() throws IOException, KeeperException, InterruptedException {
        // 创建 ZooKeeper 连接实例
        zk = new ZooKeeper(AppConstants.CONNECTIONS, sessionTimeout, watcher);

        // 获取 ZooKeeper 服务状态
        ZooKeeper.States states = zk.getState();
        log.info("sessionId = {} ,states.alive = {} ,states.connected = {} ,states.name = {}"
                , zk.getSessionId(), states.isAlive(), states.isConnected(), states.name());

        // 保险起见，初始化时也删除一次节点数据
        delete(null, root);
    }

    @After
    public void destroy() throws InterruptedException, KeeperException {
        // 删除节点，还原测试场景
        delete(null, root);
        // 关闭 ZooKeeper 连接
        if (zk != null) zk.close();
    }

    @Test
    public void usage() throws IOException, InterruptedException, KeeperException {

        // 创建一个根节点(root)
        /**
         * @param String path
         * @param byte [] data
         * @param List<org.apache.zookeeper.data.ACL> acl
         * @param org.apache.zookeeper.CreateMode createMode :
         * 		PERSISTENT				The znode will not be automatically deleted upon client's disconnect
         *		PERSISTENT_SEQUENTIAL	Its name will be appended with a monotonically increasing number
         *		EPHEMERAL				The znode will be deleted upon the client's disconnect
         *		EPHEMERAL_SEQUENTIAL	Its name will be appended with a monotonically increasing number
         */
        String msg = zk.create(root, "zlikun".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log.info("创建节点[{}]：{}", root, msg);
        log.info("查询节点[{}]下的数据：{}", root, new String(zk.getData(root, true, null)));

        // 创建一个子节点
        msg = zk.create(root + "/node-1", "N1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log.info("创建子节点：{}", msg);
        // 创建一个子节点
        msg = zk.create(root + "/node-2", "N2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log.info("创建子节点：{}", msg);

        // 检查一个节点是否存在
        Stat stat = zk.exists(root, true);

        // 如果 stat 为空，表示节点不存在
        if (stat != null) {
            System.out.print("检查节点是否存在返回状态信息：");
            System.out.println(stat);
        }

        // 遍历子节点
        printf(zk.getChildren(root, true));

        /**
         * 修改指定节点数据
         * ZooKeeper Stat 结构
         * 		czxid：该数据节点被创建时的事务id
         *		mzxid：该节点最后一次被更新时的事务id
         * 		ctime：节点被创建时的时间
         * 		mtime：节点最后一次被更新时的时间
         * 		version：这个节点的数据变化的次数
         * 		cversion：这个节点的子节点 变化次数
         * 		aversion：这个节点的ACL变化次数
         * 		ephemeralOwner：如果这个节点是临时节点，表示创建者的会话id。如果不是临时节点，这个值是0
         * 		dataLength：这个节点的数据长度
         * 		numChildren：这个节点的子节点个数
         */
        stat = zk.setData(root, "Hello ZooKeeper !".getBytes(), -1);
        System.out.print("修改节点数据返回状态信息：");
        System.out.println(stat);
        log.info("查询节点[{}]下的数据：{}", root, new String(zk.getData(root, true, stat)));

    }

    /**
     * 递归删除节点
     *
     * @param parent
     * @param node
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void delete(String parent, String node) throws KeeperException, InterruptedException {
        // 如果节点不存在，直接返回
        if (parent != null) {
            node = parent + (node.startsWith("/") ? node : "/" + node);
        }
        if (zk.exists(node, true) == null) return;
        // 查询节点是否包含子节点
        List<String> nodes = zk.getChildren(node, true);
        if (nodes != null && !nodes.isEmpty()) {
            // 当前node即为子节点的上级节点
            parent = node;
            // 迭代递归删除子节点
            for (String path : nodes) {
                delete(parent, path);
            }
        }

        // the expected node version, if the given version is -1, it matches any node's versions
        zk.delete(node, -1);
        log.debug("删除节点：{}", node);
    }

    /**
     * 打印列表日志
     *
     * @param list
     */
    private void printf(List<String> list) {
        if (list == null || list.isEmpty()) return;
        for (String item : list) {
            log.info("ChildNode => {}", item);
        }
    }

}