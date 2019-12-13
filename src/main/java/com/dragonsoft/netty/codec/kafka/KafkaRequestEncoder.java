package com.dragonsoft.netty.codec.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.dragonsoft.netty.codec.kafka.ChannelUtil.parseChannelLocalAddr;
import static com.dragonsoft.netty.codec.kafka.ChannelUtil.parseChannelRemoteAddr;
import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.LOGGER_NAME;

/**
 * @author: ronhunlam
 * date:2019/8/19 22:20
 */
public class KafkaRequestEncoder extends MessageToByteEncoder<ByteBuffer> {
	private static final Logger logger = LoggerFactory.getLogger(LOGGER_NAME);
	// left for future.
	private final Channel inboundChannel;
	
	public KafkaRequestEncoder(Channel inboundChannel) {
		this.inboundChannel = inboundChannel;
	}
	
	@Override
	protected void encode(ChannelHandlerContext ctx, ByteBuffer response, ByteBuf byteBuf) throws Exception {
		try {
			byteBuf.writeInt(response.remaining());
			byteBuf.writeBytes(response);
		} catch (Exception e) {
			logger.error("inbound channel local address {} remote address {} request encode exception {}",
				parseChannelLocalAddr(ctx.channel()), parseChannelRemoteAddr(ctx.channel()), e);
			ChannelUtil.closeChannel(ctx.channel());
		}
	}
}