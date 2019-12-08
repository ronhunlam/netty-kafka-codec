package com.dragonsoft.netty.codec.kafka;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.LOGGER_NAME;

/**
 * @author: ronhunlam
 * date:2019/8/19 16:29
 */
public class KafkaNettyProxyBackendHandler extends ChannelInboundHandlerAdapter {
	
	private static Logger logger = LoggerFactory.getLogger(LOGGER_NAME);
	private Channel inboundChannel;
	private ResponseConvert responseConvert;
	
	
	public KafkaNettyProxyBackendHandler(Channel inboundChannel) {
		this(inboundChannel, new DefaultResponseConvert());
	}
	
	public KafkaNettyProxyBackendHandler(Channel inboundChannel, ResponseConvert convert) {
		this.inboundChannel = inboundChannel;
		responseConvert = convert;
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("outbound channel active: " + ChannelUtil.parseChannelRemoteAddr(ctx.channel()));
		ctx.channel().read();
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof KafkaNettyResponse) {
			KafkaNettyResponse response = (KafkaNettyResponse) msg;
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
		ChannelUtil.closeChannel(inboundChannel);
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		ChannelUtil.closeChannel(inboundChannel);
	}
	
	private void saveRawMetadatResponse() {
	
	}
}
