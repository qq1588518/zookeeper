package com.zlikun.apache;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2017/8/16 10:28
 */
public interface Consts {
    String HOST = "192.168.121.93" ;	// 使用Docker构建的Zookeeper集群
    String CONNECTIONS = String.format("%s:%d,%s:%d,%s:%d" ,HOST ,2181 ,HOST ,2182 ,HOST ,2183) ;
}
