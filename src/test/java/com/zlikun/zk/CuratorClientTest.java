package com.zlikun.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorClientTest {

	private Logger log = LoggerFactory.getLogger(CuratorClientTest.class) ;
	
	private String connectString = "192.168.60.210:2181" ;
	private CuratorFramework client ;
	
	@Before
	public void init() {
		client = CuratorFrameworkFactory.builder()
				.connectString(connectString)
				.connectionTimeoutMs(30000)
				.sessionTimeoutMs(30000)
				.canBeReadOnly(false)
				.retryPolicy(new RetryNTimes(1000, 100))
				.namespace("zlikun")
				.defaultData(null)
				.build() ;
		// 连接状态监听
		client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState state) {
				System.out.println(state);
			}
		});
		client.start();
		// 判断/zlikun/curator目录是否存在，存在则删除，避免干扰后续测试
		try {
			client.delete().forPath("/curator") ;
		} catch (Exception e) {
			log.error("删除节点出错!" ,e);
		}
	}
	
	@Test
	public void test() {
		
		try {
			client.create().forPath("/curator" ,"Hello".getBytes()) ;
			System.out.println(new String (client.getData().forPath("/curator")));
		} catch (Exception e) {
			e.printStackTrace();
		}

		
		
	}
	
	@After
	public void destroy() {
		CloseableUtils.closeQuietly(client);
	}
	
}