package com.dragonsoft.netty.codec.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyFrontendHandler.requestKey;
import static com.dragonsoft.netty.codec.kafka.OriginRequestBuffer.getOriginRequest;

/**
 * @author: ronhunlam
 * date:2019/8/19 17:10
 */
public class KafkaResponseDecoder extends LengthFieldBasedFrameDecoder {
	
	private static Logger logger = LoggerFactory.getLogger(KafkaRequestDecoder.class.getName());
	private static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024;
	private Channel inboundChannel;
	
	public KafkaResponseDecoder(Channel inboundChannel) {
		super(MAX_FRAME_LENGTH, 0, 4, 0, 4);
		this.inboundChannel = inboundChannel;
	}
	
	@Override
	protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		ByteBuf frame = null;
		KafkaNettyResponse response = null;
		try {
			frame = (ByteBuf) super.decode(ctx, in);
			ByteBuffer responseBuffer = frame.nioBuffer();
			logger.info("outbound begin to read response ================> ");
			String channelId = ctx.channel().id().asShortText();
			KafkaNettyRequest request = inboundChannel.attr(requestKey).get();
			if (request != null) {
				// the response is corresponding to the request :).
				RequestHeader requestHeader = request.getRequestHeader();
				ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);
				Struct responseStruct = requestHeader.apiKey().parseResponse(requestHeader.apiVersion(), responseBuffer);
				AbstractResponse responsBody = AbstractResponse.
					parseResponse(requestHeader.apiKey(), responseStruct, requestHeader.apiVersion());
				if (responsBody instanceof MetadataResponse) {
					MetadataCache.setCache((MetadataResponse)responsBody);
				}
				if (responsBody instanceof FindCoordinatorResponse) {
					FindCoordinatorResponse coordinatorResponse = (FindCoordinatorResponse) responsBody;
					CoordinatorCache.putCoordinator(coordinatorResponse.node().idString(), coordinatorResponse);
				}
				response = new KafkaNettyResponse(request, responseHeader, responsBody);
				logger.info("outbound read response ================> " + response.toString());
			}
		} catch (Exception e) {
			logger.error("encode exception, " + ChannelUtil.parseChannelRemoteAddr(ctx.channel()), e);
			ChannelUtil.closeChannel(ctx.channel());
		} finally {
			if (null != frame) {
				frame.release();
			}
		}
		return response;
	}
}
