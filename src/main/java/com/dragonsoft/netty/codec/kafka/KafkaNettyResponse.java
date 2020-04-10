package com.dragonsoft.netty.codec.kafka;

import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

/**
 * @author: ronhunlam
 * date:2019/8/15 17:10
 */
public class KafkaNettyResponse {
	/** request，用来和响应关联在一起 */
	private KafkaNettyRequest request;
	/** response header */
	private ResponseHeader responseHeader;
	/** response body */
	private AbstractResponse responseBody;
	
	public KafkaNettyResponse(KafkaNettyRequest request, ResponseHeader responseHeader, AbstractResponse response) {
		this.request = request;
		this.responseHeader = responseHeader;
		this.responseBody = response;
	}
	
	public KafkaNettyRequest getRequest() {
		return request;
	}
	
	public ResponseHeader getResponseHeader() {
		return responseHeader;
	}
	
	public AbstractResponse getResponseBody() {
		return responseBody;
	}
	
	@Override
	public String toString() {
		RequestHeader requestHeader = request.getRequestHeader();
		return "Response Header ====> " + request.getRequestHeader().toStruct().toString() +
			    "Response Body  =====> " + responseBody.toString(requestHeader.apiVersion());
	}
}
