package com.dragonsoft.netty.codec.kafka;

import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.MetadataResponse;

import java.nio.ByteBuffer;

import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.localProxyPort;
import static com.dragonsoft.netty.codec.kafka.NetworkUtil.getLocalHostName;

/**
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
				modifyFindGroupResponse((FindCoordinatorResponse) rawResponse);
				break;
			case UPDATE_METADATA:
				break;
		}
		ByteBuffer resonseBuffer = rawResponse.serialize(request.apiVersion(), response.getResponseHeader());
		return resonseBuffer;
	}
	
	/**
	 * replace the hosts and ports of {@link MetadataResponse} returned from server with local host and port.
	 *
	 * @param mr
	 * @return
	 * @throws
	 */
	private void modifyMetadataResponse(MetadataResponse mr) {
		MetadataResponseData mrp = mr.data();
		MetadataResponseData.MetadataResponseBrokerCollection mrdmrbc = mrp.brokers();
		for (MetadataResponseData.MetadataResponseBroker broker : mrdmrbc) {
			broker.setHost(NetworkUtil.getLocalHostName());
			broker.setPort(localProxyPort);
		}
	}
	
	/**
	 * replace the hosts and ports of {@link FindCoordinatorResponse} returned from server with local host and port.
	 *
	 * @param fr
	 * @return
	 * @throws
	 */
	private void modifyFindGroupResponse(FindCoordinatorResponse fr) {
		FindCoordinatorResponseData frData = fr.data();
		frData.setHost(getLocalHostName());
		frData.setPort(localProxyPort);
	}
}
