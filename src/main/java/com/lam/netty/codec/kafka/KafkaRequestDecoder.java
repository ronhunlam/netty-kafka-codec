package com.lam.netty.codec.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.lam.netty.codec.kafka.ChannelUtil.getInboundChannel;
import static com.lam.netty.codec.kafka.KafkaNettyProxyConfig.MAX_FRAME_LENGTH;

/**
 * handler for inbound channel.
 *
 * @author: ronhunlam
 * date:2019/8/2 18:58
 */
public class KafkaRequestDecoder extends LengthFieldBasedFrameDecoder {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaRequestDecoder.class);
	
	public KafkaRequestDecoder() {
		super(MAX_FRAME_LENGTH, 0, 4, 0, 4);
	}
	
	/**
	 * 将 {@link ByteBuffer in} 转换为 {@link AbstractRequest},并包装为 {@link KafkaNettyRequest} 对象
	 *
	 * @param
	 * @return Object
	 * @throws
	 */
	@Override
	protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		ByteBuf frame = null;
		KafkaNettyRequest request = null;
		try {
			// if the frame is null, that indicates the tcp segments are being sliced.
			frame = (ByteBuf) super.decode(ctx, in);
			if (frame != null) {
				ByteBuffer rawBuffer = frame.nioBuffer();
				RequestHeader header = RequestHeader.parse(rawBuffer);
				RequestContext requestContext = new RequestContext(header, header.clientId(), null, null, null, null);
				RequestAndSize bodyAndSize = requestContext.parseRequest(rawBuffer);
				AbstractRequest requestBody = bodyAndSize.request;
				request = new KafkaNettyRequest(header, requestBody);
				logger.info("inbound channel {} read {} request ==========> {}", getInboundChannel(ctx.channel()), header.apiKey()
					, request.toString());
			}
		} catch (Exception e) {
			logger.error("inbound channel {} decoding request occurs exception: {}",
				getInboundChannel(ctx.channel()), e);
			ChannelUtil.closeChannel(ctx.channel());
		} finally {
			if (null != frame) {
				frame.release();
			}
		}
		return request;
	}
}
