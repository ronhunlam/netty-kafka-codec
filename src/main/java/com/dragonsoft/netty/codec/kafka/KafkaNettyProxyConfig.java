package com.dragonsoft.netty.codec.kafka;

/**
 * @author: ronhunlam
 * date:2019/8/15 10:42
 */
public class KafkaNettyProxyConfig {
	
	public static final String DEFAULT_CHARSET = "UTF-8";
	public static final String LOGGER_NAME = "KafkaNettyProxyServer";
	public static final int CHANNEL_MAXIDLE_TIMESECONDS = 120;
	//public static final String KAFKA_HOST = "192.168.0.103";
	//public static final String KAFKA_HOST = "10.100.2.233";
	//public static final String KAFKA_HOST = "10.100.3.21";
	public static final String KAFKA_HOST_CONFIG = "netty.proxy.kafka.host";
	public static final String KAFKA_PORT_CONFIG = "netty.proxy.kafka.port";
	
	public static final String NETTY_PROXY_PORT = "netty.proxy.port";
	
	//public static final String KAFKA_HOST = System.getProperty(KAFKA_HOST_CONFIG, "localhost");
	public static final String KAFKA_HOST = "10.100.3.21";
	public static final int KAFKA_PORT = Integer.parseInt(System.getProperty(KAFKA_PORT_CONFIG, "9092"));
	
	public static final int localProxyPort = Integer.parseInt(System.getProperty(NETTY_PROXY_PORT, "9092"));
}
