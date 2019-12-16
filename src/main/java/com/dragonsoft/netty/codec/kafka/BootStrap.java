package com.dragonsoft.netty.codec.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bootstrap the proxy server
 * @author: ronhunlam
 * date:2019/8/2 9:46
 */
public class BootStrap {
	
	private static final Logger logger = LoggerFactory.getLogger(BootStrap.class);
	
	public static void main(String[] args) {
		ProxyServer server = null;
		try {
			server = new KafkaNettyProxyServer();
			server.start();
			logger.info("start the netty kafka proxy...");
		} catch (Exception e) {
			logger.error("the netty kafka proxy encounters exception,begin to shutdown", e);
			server.shutdown();
			System.exit(-1); // issue error signal
		}
	}
}
