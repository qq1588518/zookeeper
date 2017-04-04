package com.zlikun.zk;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorClientTest {

	private Logger log = LoggerFactory.getLogger(CuratorClientTest.class) ;
	
	private CuratorFramework client ;
	
	private String path = "/zlikun" ;
	
	@Before
	public void init() {
		client = CuratorFrameworkFactory.builder()
				.connectString(Consts.HOST_PORT)
				.connectionTimeoutMs(30000)
				.sessionTimeoutMs(30000)
				.canBeReadOnly(false)
				.retryPolicy(new RetryNTimes(1000, 100))
				.namespace("curator")
				.defaultData(null)
				.build() ;
		// 连接状态监听
		client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState state) {
				log.info("connection state : {}" ,state);
			}
		});
		// 添加CuratorListener监听
		client.getCuratorListenable().addListener(new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
				log.info("CuratorEvent : {}" ,event);
			}
		});
		client.start();
		// 判断/zlikun/curator目录是否存在，存在则删除，避免干扰后续测试
		try {
			// 判断节点是否存在，返回状态对象
			Stat stat = client.checkExists().forPath(path) ;
			if(stat != null) {
				// 判断是否有子节点，先删除子节点
				DeleteBuilder db = client.delete() ;
				for(String cpath : client.getChildren().forPath(path)) {
					db.forPath(path + "/" + cpath) ;
				}
				db.forPath(path) ;
			}
		} catch (Exception e) {
			log.error("删除节点出错!" ,e);
		}
	}
	
	@Test
	public void test() throws Exception {
		
		// 连接客户端状态
		log.info("client state : {}" ,client.getState());
		
		// 创建节点
		String msg = client.create().forPath(path ,"Hello".getBytes()) ;
		// 创建节点消息：/zlikun
		log.info("创建节点消息：{}" ,msg);
		
		// 查询节点数据
		// /zlikun节点数据：Hello
		log.info("{}节点数据：{}" ,path ,new String (client.getData().forPath(path)));
		
		// 查询节点访问控制列表
		List<ACL> aclList = client.getACL().forPath(path) ;
		// /zlikun ACL : 31,s{'world,'anyone}
		for(ACL acl : aclList) {
			log.info("{} ACL : {}" ,path ,acl);
		}
		
		// 创建子节点
		CreateBuilder cb = client.create() ;
		// 设置模式(临时节点不允许拥有子节点)
//		cb.withMode(CreateMode.EPHEMERAL_SEQUENTIAL) ;
		cb.withMode(CreateMode.EPHEMERAL) ;
		log.info("创建子节点 : {}" ,cb.forPath(path + "/100")) ;	// 创建子节点 : /zlikun/100
		log.info("创建子节点 : {}" ,cb.forPath(path + "/101")) ;	// 创建子节点 : /zlikun/101
		log.info("创建子节点 : {}" ,cb.forPath(path + "/102")) ;	// 创建子节点 : /zlikun/102
		
		// 查询子节点
		List<String> children = client.getChildren().forPath(path) ;
		for(String c : children) {
			log.info("{} 子节点 : {}" ,path ,c);
		}

		// 删除节点
		client.delete().forPath(path + "/100") ;
		
	}
	
	@After
	public void destroy() {
		CloseableUtils.closeQuietly(client);
	}
	
}