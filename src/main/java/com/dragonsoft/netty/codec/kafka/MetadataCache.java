package com.dragonsoft.netty.codec.kafka;

import org.apache.kafka.common.requests.MetadataResponse;

import java.util.concurrent.ConcurrentHashMap;

/**
 * cache the {@code Metadata}
 *
 * @author: ronhunlam
 * date:2019/12/7 16:12
 */
public class MetadataCache {
	
	private static volatile MetadataResponse metadataResponse;
	
	private static final ConcurrentHashMap<Integer, NodeWrapper> rawNodesInfos = new ConcurrentHashMap<>();
	
	private MetadataCache() {
	
	}
	
	public static void setCache(MetadataResponse response) {
		metadataResponse = response;
	}
	
	public static MetadataResponse getCache() {
		return metadataResponse;
	}
	
	public static void putNodeInfo(int nodeId, NodeWrapper wrapper) {
		rawNodesInfos.put(nodeId, wrapper);
	}
	
	public static NodeWrapper getNodeInfo(int nodeId) {
		return rawNodesInfos.get(nodeId);
	}
}
