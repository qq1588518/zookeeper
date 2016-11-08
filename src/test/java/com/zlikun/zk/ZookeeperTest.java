package com.zlikun.zk;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperTest {

	private static final Logger log = LoggerFactory.getLogger(ZookeeperTest.class) ;
	
	private String server = "192.168.60.210:2181" ;
//	private String server = "192.168.9.204:2181,192.168.9.206:2181" ;
	
	@Test
	public void test() throws IOException, InterruptedException, KeeperException {

		// zk连接字符串
		String connectString = server ;
		// zk会话超时设置，单位：毫秒
		int sessionTimeout = 3000 ;
		// zk观察者参数
		Watcher watcher = new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				log.info("已经触发了{}事件！" ,event.getType());
			}
		} ;
		
		// 创建一个与服务器的连接
		ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, watcher);
		log.info("ZooKeeper状态：{}" ,zk.getState().name());
		log.info("ZooKeeper会话ID：{}" ,zk.getSessionId());

		// 事先删除节点，避免后续创建出错
		try {
			for(String path : zk.getChildren("/zlikun", false)) {
				zk.delete("/zlikun/" + path, -1);
			}
			zk.delete("/zlikun", -1);
		} catch (Exception e) {
			
		}
		
		// 创建一个目录节点
		/**
		 * CreateMode:
		 * 		PERSISTENT				The znode will not be automatically deleted upon client's disconnect
		 *		PERSISTENT_SEQUENTIAL	Its name will be appended with a monotonically increasing number
		 *		EPHEMERAL				The znode will be deleted upon the client's disconnect
		 *		EPHEMERAL_SEQUENTIAL	Its name will be appended with a monotonically increasing number
		 */
		String msg = zk.create("/zlikun", "zlikun".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) ;
		log.info("创建目录节点[/zlikun]：{}" ,msg);
		log.info("目录节点[/zlikun]下的数据：{}", new String(zk.getData("/zlikun", false, null))); 
		
		// 创建一个子节点
		msg = zk.create("/zlikun/c0", "c0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) ;
		log.info("创建目录节点[/zlikun/c0]：{}" ,msg);
		// 创建一个子节点
		msg = zk.create("/zlikun/c2", "c2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) ;
		
		// 获取指定目录所有子节点
		List<String> c0List = zk.getChildren("/zlikun", true) ;
		printf(c0List) ;
		
		// 修改子目录节点数据
		/**
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
		Stat stat = zk.setData("/zlikun/c2", "zlikun.c2".getBytes(), -1) ;
		log.info("修改节点[/zlikun/c2]下数据：{}" ,stat.toString());
		log.info("目录节点[/zlikun/c2]下的数据：{}", new String(zk.getData("/zlikun/c2", false, null)));
		
		// 删除一个目录节点
		// the expected node version
		// if the given version is -1, it matches any node's versions
		int version = -1 ;
		// 删除节点如果不存在，将抛出org.apache.zookeeper.KeeperException$NoNodeException异常
		zk.delete("/zlikun/c0", version);
		zk.delete("/zlikun/c2", version);
		zk.delete("/zlikun", version);
		// 判断节点是否存在
		stat = zk.exists("/zlikun/c3", true) ;
		if(stat != null) {
			zk.delete("/zlikun/c3", version);
		}
		
//		// Zookeeper事务
//		Transaction t = zk.transaction() ;
//		t.create(path, data, acl, createMode)
//		List<OpResult> orList = t.commit() ;
		
		// 关闭连接
		if(zk != null) zk.close();

	}
	
	/**
	 * 打印列表日志
	 * @param list
	 */
	private void printf(List<String> list) {
		if(list == null || list.isEmpty()) return ;
		for(String item : list) {
			log.info("=> {}" ,item);
		}
	}

}