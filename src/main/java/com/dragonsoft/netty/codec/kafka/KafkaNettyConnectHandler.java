package com.dragonsoft.netty.codec.kafka;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.LOGGER_NAME;

/**
 * @author: ronhunlam
 * date:2019/8/15 13:57
 */
public class KafkaNettyConnectHandler extends ChannelDuplexHandler {
	private static Logger logger = LoggerFactory.getLogger(LOGGER_NAME);
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		final String remoteAddr = ChannelUtil.parseChannelRemoteAddr(ctx.channel());
		logger.info("Channel to remoteAddress {} is registered", remoteAddr);
		super.channelRegistered(ctx);
	}
	
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		final String remoteAddr = ChannelUtil.parseChannelRemoteAddr(ctx.channel());
		logger.info("Channel to remoteAddress {} is unregistered", remoteAddr);
		super.channelUnregistered(ctx);
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		final String remoteAddr = ChannelUtil.parseChannelRemoteAddr(ctx.channel());
		logger.info("Channel to remoteAddress {} is active", remoteAddr);
		super.channelActive(ctx);
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		final String remoteAddr = ChannelUtil.parseChannelRemoteAddr(ctx.channel());
		logger.info("Channel to remoteAddress {} is inactive", remoteAddr);
		super.channelInactive(ctx);
	}
	
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent stateEvent = (IdleStateEvent) evt;
			if (stateEvent.state() == IdleState.ALL_IDLE) {
				final String remoteAddress = ChannelUtil.parseChannelRemoteAddr(ctx.channel());
				logger.info("connection to remoteAddress {} is all idle", remoteAddress);
				ChannelUtil.closeChannel(ctx.channel());
			}
		}
		ctx.fireUserEventTriggered(evt);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		final String remoteAddr = ChannelUtil.parseChannelRemoteAddr(ctx.channel());
		logger.info("Channel to remoteAddress {} is exceptional", remoteAddr, cause);
		ChannelUtil.closeChannel(ctx.channel());
	}
}
