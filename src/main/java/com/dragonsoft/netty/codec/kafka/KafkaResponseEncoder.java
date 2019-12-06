package com.dragonsoft.netty.codec.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.LOGGER_NAME;

/**
 * @author: ronhunlam
 * date:2019/8/2 18:58
 */
public class KafkaResponseEncoder extends MessageToByteEncoder<ByteBuffer> {
	private static final Logger logger = LoggerFactory.getLogger(LOGGER_NAME);
	
	@Override
	protected void encode(ChannelHandlerContext ctx, ByteBuffer response, ByteBuf byteBuf) throws Exception {
		try {
			byteBuf.writeInt(response.remaining());
			byteBuf.writeBytes(response);
		} catch (Exception e) {
			logger.error("encode exception, " + ChannelUtil.parseChannelRemoteAddr(ctx.channel()), e);
			ChannelUtil.closeChannel(ctx.channel());
		}
	}
}
