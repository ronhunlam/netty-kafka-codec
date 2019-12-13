package com.dragonsoft.netty.codec.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Queue;

import static com.dragonsoft.netty.codec.kafka.ChannelUtil.parseChannelLocalAddr;
import static com.dragonsoft.netty.codec.kafka.ChannelUtil.parseChannelRemoteAddr;

/**
 * @author: ronhunlam
 * date:2019/8/19 17:10
 */
public class KafkaResponseDecoder extends LengthFieldBasedFrameDecoder {
	
	private static Logger logger = LoggerFactory.getLogger(KafkaRequestDecoder.class.getName());
	private static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024;
	// left for future
	private final Channel inboundChannel;
	private final Queue<KafkaNettyRequest> cachedRequests;
	
	public KafkaResponseDecoder(Channel inboundChannel, Queue<KafkaNettyRequest> cachedRequests) {
		super(MAX_FRAME_LENGTH, 0, 4, 0, 4);
		this.inboundChannel = inboundChannel;
		this.cachedRequests = cachedRequests;
	}
	
	@Override
	protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		ByteBuf frame = null;
		KafkaNettyResponse response = null;
		try {
			frame = (ByteBuf) super.decode(ctx, in);
			// if the frame is null, that indicates the tcp segment is sliced.
			if (frame != null) {
				ByteBuffer responseBuffer = frame.nioBuffer();
				KafkaNettyRequest request = cachedRequests.poll();
				if (request != null) {
					// the response is corresponding to the request :).
					logger.info("outbound begin to read {} response", request.getRequestHeader().apiKey());
					RequestHeader requestHeader = request.getRequestHeader();
					ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);
					Struct responseStruct = requestHeader.apiKey().parseResponse(requestHeader.apiVersion(), responseBuffer);
					AbstractResponse responsBody = AbstractResponse.
						parseResponse(requestHeader.apiKey(), responseStruct, requestHeader.apiVersion());
					response = new KafkaNettyResponse(request, responseHeader, responsBody);
					logger.info("outbound channel local address {} remote address {} read {} ==========> response {}",
						parseChannelLocalAddr(ctx.channel()), parseChannelRemoteAddr(ctx.channel()),
						request.getRequestHeader().apiKey(), response.toString());
				}
			}
		} catch (Exception e) {
			logger.error("outbound channel local address {} remote address {} decoding response occurs exception: {}",
				parseChannelLocalAddr(ctx.channel()), parseChannelRemoteAddr(ctx.channel()), e);
		} finally {
			if (null != frame) {
				frame.release();
			}
		}
		return response;
	}
}
