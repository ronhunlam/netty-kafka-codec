package com.lam.netty.codec.kafka;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;

/**
 * @author: ronhunlam
 * date:2019/12/6 15:49
 */
public class DefaultRequestConvert implements RequestConvert {
	
	@Override
	public ByteBuffer convertRequestToBuffer(KafkaNettyRequest request) {
		
		RequestHeader header = request.getRequestHeader();
		AbstractRequest requestBody = request.getRequestBody();
		ByteBuffer requestBuffer = requestBody.serialize(header);
		return requestBuffer;
	}
}
