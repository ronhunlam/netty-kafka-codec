package com.dragonsoft.netty.codec.kafka;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

/**
 * @author: ronhunlam
 * date:2019/8/15 16:42
 */
public class KafkaNettyRequest {
	/**
	 * request header
	 */
	private RequestHeader requestHeader;
	/**
	 * request body
	 */
	private AbstractRequest requestBody;
	
	public KafkaNettyRequest(RequestHeader requestHeader, AbstractRequest requestBody) {
		this.requestHeader = requestHeader;
		this.requestBody = requestBody;
	}
	
	/** 获取请求头
	 * @param
	 * @return
	 * @throws
	*/
	public RequestHeader getRequestHeader() {
		return requestHeader;
	}
	
	/** 获取请求体
	 * @param
	 * @return
	 * @throws
	*/
	public AbstractRequest getRequestBody() {
		return requestBody;
	}
	
	/** 判断服务端是否支持客户端的 Api 版本
	 * @param
	 * @return
	 * @throws
	*/
	private boolean isUnsupportedApiVersionsRequest() {
		return requestHeader.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(requestHeader.apiVersion());
	}
	
	/** 把 {@link ResponseHeader}和{@link AbstractResponse body} 序列化为 {@code ByteBuffer}，以便写入到 {@link KafkaResponseEncoder} 中
	 * @param body
	 * @return ByteBuffer
	 * @throws
	*/
	public ByteBuffer buildResponse(AbstractResponse body) {
		ResponseHeader responseHeader = requestHeader.toResponseHeader();
		return body.serialize(apiVersion(), responseHeader);
	}
	
	/** 获取 Api 请求的版本号,如果服务端不支持此版本，则返回 0
	 * @param
	 * @return short
	 * @throws
	*/
	public short apiVersion() {
		// Use v0 when serializing an unhandled ApiVersion response
		if (isUnsupportedApiVersionsRequest())
			return 0;
		return requestHeader.apiVersion();
	}
	
	@Override
	public String toString() {
		return requestHeader.toString() + " => requestBody[" + requestBody.toString() + "]";
	}
}
