package com.dragonsoft.netty.codec.kafka;

import org.apache.kafka.common.requests.FindCoordinatorResponse;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: ronhunlam
 * date:2019/12/7 20:34
 */
public class CoordinatorCache {
	
	private static ConcurrentHashMap<String, FindCoordinatorResponse> cache = new ConcurrentHashMap<>();
	
	private CoordinatorCache() {
	
	}
	
	public static FindCoordinatorResponse getCoordinator(String groupId) {
		return cache.get(groupId);
	}
	
	public static void putCoordinator(String groudId, FindCoordinatorResponse response) {
		cache.put(groudId, response);
	}
}
