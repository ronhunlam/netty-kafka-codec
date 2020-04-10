package com.dragonsoft.netty.codec.kafka;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.dragonsoft.netty.codec.kafka.ChannelUtil.getInboundChannel;
import static com.dragonsoft.netty.codec.kafka.ChannelUtil.getOutboundChannel;

/**
 * this handler is used for converting {@link KafkaNettyResponse} to {@link ByteBuffer}.
 *
 * @author: ronhunlam
 * date:2019/8/19 16:29
 */
public class KafkaNettyProxyBackendHandler extends ChannelInboundHandlerAdapter {
	
	private static Logger logger = LoggerFactory.getLogger(KafkaNettyProxyBackendHandler.class);
	
	private final Channel inboundChannel;
	private final ResponseConvert responseConvert;
	
	
	public KafkaNettyProxyBackendHandler(Channel inboundChannel) {
		this(inboundChannel, new DefaultResponseConvert());
	}
	
	public KafkaNettyProxyBackendHandler(Channel inboundChannel, ResponseConvert convert) {
		this.inboundChannel = inboundChannel;
		responseConvert = convert;
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("outbound channel {} corresponding inbound channel {} active",
			getOutboundChannel(ctx.channel()), getInboundChannel(inboundChannel));
		ctx.channel().read();
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg != null && msg instanceof KafkaNettyResponse) {
			KafkaNettyResponse response = (KafkaNettyResponse) msg;
			logger.info("outbound channel {} corresponding inbound channel {} reads response {}",
				getOutboundChannel(ctx.channel()), getInboundChannel(inboundChannel), response);
			ByteBuffer resonseBuffer = responseConvert.convertResponseToBuffer(response);
			inboundChannel.writeAndFlush(resonseBuffer).addListener((ChannelFutureListener) future -> {
				if (future.isSuccess()) {
					ctx.channel().read();
				} else {
					future.channel().close();
				}
			});
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.info("outbound channel {} corresponding inbound channel {} occurs exception {}",
			getOutboundChannel(ctx.channel()), getInboundChannel(inboundChannel), cause);
		ChannelUtil.closeChannel(ctx.channel());
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		logger.info("outbound channel {} corresponding inbound channel {} inactive",
			getOutboundChannel(ctx.channel()), getInboundChannel(inboundChannel));
	}
}
