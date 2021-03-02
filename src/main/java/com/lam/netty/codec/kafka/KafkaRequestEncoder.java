package com.lam.netty.codec.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.lam.netty.codec.kafka.ChannelUtil.*;

/** handler for outbound channel.
 * @author: ronhunlam
 * date:2019/8/19 22:20
 */
public class KafkaRequestEncoder extends MessageToByteEncoder<ByteBuffer> {
	private static final Logger logger = LoggerFactory.getLogger(KafkaRequestEncoder.class);
	
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
			logger.error("outbound channel {} corresponding inbound channel {} request encode exception {}",
				getOutboundChannel(ctx.channel()), getInboundChannel(inboundChannel), e);
			ChannelUtil.closeChannel(ctx.channel());
		}
	}
}