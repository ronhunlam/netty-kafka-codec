package com.dragonsoft.netty.codec.kafka;

import io.netty.util.internal.StringUtil;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: ronhunlam
 * date:2019/12/2 22:10
 */
public class OriginRequestBuffer {
	
	private static Map<String, Queue<KafkaNettyRequest>> outBoundChannelRequest = new ConcurrentHashMap<>();
	
	private OriginRequestBuffer() {
	
	}
	
	/**
	 * add {@link KafkaNettyRequest request}
	 *
	 * @param channelId
	 * @param request
	 * @return
	 * @throws
	 */
	public static void addOriginRequest(String channelId, KafkaNettyRequest request) {
		if (StringUtil.isNullOrEmpty(channelId)) {
			throw new IllegalArgumentException("The channelId can't be empty!");
		}
		Queue<KafkaNettyRequest> requestQueue = outBoundChannelRequest.get(channelId);
		if (requestQueue == null) {
			requestQueue = new LinkedList<>();
			outBoundChannelRequest.putIfAbsent(channelId, requestQueue);
		}
		outBoundChannelRequest.get(channelId).add(request);
	}
	
	/**
	 * get the {@link KafkaNettyRequest}
	 *
	 * @param channelId
	 * @return {@link KafkaNettyRequest}
	 * @throws
	 */
	public static KafkaNettyRequest getOriginRequest(String channelId) {
		if (StringUtil.isNullOrEmpty(channelId)) {
			throw new IllegalArgumentException("channelId can't be empty!");
		}
		Queue<KafkaNettyRequest> queue = outBoundChannelRequest.get(channelId);
		return queue.poll();
	}
	
	public static void clearOutboundRequest() {
		outBoundChannelRequest.clear();
	}
	
	public static void clearAll() {
		clearOutboundRequest();
	}
}
