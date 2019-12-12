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
	public static final String KAFKA_HOST = "10.100.2.233";
	public static final int KAFKA_PORT = 9092;
	
	public static final int localProxyPort = 9092;
}
