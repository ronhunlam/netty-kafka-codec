package com.dragonsoft.netty.codec.kafka;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.MetadataResponse;

import java.nio.ByteBuffer;

import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.LOCAL_PROXY_PORT;
import static com.dragonsoft.netty.codec.kafka.NetworkUtil.getLocalHostName;

/**
 * convert {@link KafkaNettyResponse} to {@link ByteBuffer}
 *
 * @author: ronhunlam
 * date:2019/12/6 16:02
 */
public class DefaultResponseConvert implements ResponseConvert {
	
	@Override
	public ByteBuffer convertResponseToBuffer(KafkaNettyResponse response) {
		
		KafkaNettyRequest request = response.getRequest();
		AbstractResponse rawResponse = response.getResponseBody();
		ApiKeys requestApiKeys = request.getRequestHeader().apiKey();
		switch (requestApiKeys) {
			case METADATA:
				modifyMetadataResponse((MetadataResponse) rawResponse);
				break;
			case FIND_COORDINATOR:
				String groupId = ((FindCoordinatorRequest) request.getRequestBody()).data().key();
				modifyFindGroupResponse(groupId, (FindCoordinatorResponse) rawResponse);
				break;
			case UPDATE_METADATA:
				break;
		}
		ByteBuffer resonseBuffer = rawResponse.serialize(request.apiVersion(), response.getResponseHeader());
		return resonseBuffer;
	}
	
	/**
	 * replace the hosts and ports of {@link MetadataResponse} returned from server with proxy host and port.
	 *
	 * @param mr
	 * @return
	 * @throws
	 */
	private void modifyMetadataResponse(MetadataResponse mr) {
		MetadataCache.setCache(mr);
		MetadataResponseData mrp = mr.data();
		MetadataResponseData.MetadataResponseBrokerCollection mrdmrbc = mrp.brokers();
		for (MetadataResponseData.MetadataResponseBroker broker : mrdmrbc) {
			MetadataCache.putNodeInfo(broker.nodeId(), new NodeWrapper(broker.nodeId(), broker.host(), broker.port()));
			broker.setHost(NetworkUtil.getLocalHostName());
			broker.setPort(LOCAL_PROXY_PORT);
		}
	}
	
	/**
	 * replace the hosts and ports of {@link FindCoordinatorResponse} returned from server with local host and port.
	 *
	 * @param fr
	 * @return
	 * @throws
	 */
	private void modifyFindGroupResponse(String groupId, FindCoordinatorResponse fr) {
		Node coordinatorNode = fr.node();
		CoordinatorCache.putCoordinator(groupId,
			new NodeWrapper(coordinatorNode.id(), coordinatorNode.host(), coordinatorNode.port()));
		FindCoordinatorResponseData frData = fr.data();
		frData.setHost(getLocalHostName());
		frData.setPort(LOCAL_PROXY_PORT);
	}
}
