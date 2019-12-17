package com.dragonsoft.netty.codec.kafka;

import java.util.concurrent.ConcurrentHashMap;

/** cache {@link org.apache.kafka.common.requests.FindCoordinatorResponse}
 * @author: ronhunlam
 * date:2019/12/7 20:34
 */
public class CoordinatorCache {
	
	private static final ConcurrentHashMap<String, NodeWrapper> cache = new ConcurrentHashMap<>();
	
	private CoordinatorCache() {
	
	}
	
	public static NodeWrapper getCoordinator(String groupId) {
		return cache.get(groupId);
	}
	
	public static void putCoordinator(String groudId, NodeWrapper coordinator) {
		cache.put(groudId, coordinator);
	}
}
