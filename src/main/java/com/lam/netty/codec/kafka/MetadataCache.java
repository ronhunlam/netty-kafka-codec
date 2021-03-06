package com.lam.netty.codec.kafka;

import org.apache.kafka.common.requests.MetadataResponse;

import java.util.concurrent.ConcurrentHashMap;

/**
 * cache {@link MetadataResponse}
 *
 * @author: ronhunlam
 * date:2019/12/7 16:12
 */
public class MetadataCache {
	
	// assume that the proxy server only surrogates a single cluster.
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
