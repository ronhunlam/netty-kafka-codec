package com.dragonsoft.netty.codec.kafka;

import org.apache.kafka.common.requests.MetadataResponse;

/** cache the {@code Metadata}
 * @author: ronhunlam
 * date:2019/12/7 16:12
 */
public class MetadataCache {
	
	private static volatile MetadataResponse metadataResponse;
	
	private MetadataCache() {
	
	}
	
	public static void setCache(MetadataResponse response) {
		metadataResponse = response;
	}
	
	public static MetadataResponse getCache() {
		return metadataResponse;
	}
}
