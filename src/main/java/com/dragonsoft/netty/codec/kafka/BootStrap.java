package com.dragonsoft.netty.codec.kafka;

/**
 * @author: ronhunlam
 * date:2019/8/2 9:46
 */
public class BootStrap {
	
	public static void main(String[] args) {
		ProxyServer server = null;
		try {
			server = new KafkaNettyProxyServer();
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
			server.shutdown();
		}
	}
}
