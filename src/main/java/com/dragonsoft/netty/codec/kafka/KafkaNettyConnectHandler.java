package com.dragonsoft.netty.codec.kafka;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dragonsoft.netty.codec.kafka.ChannelUtil.getInboundChannel;

/** Connection manage handler
 * @author: ronhunlam
 * date:2019/8/15 13:57
 */
public class KafkaNettyConnectHandler extends ChannelDuplexHandler {
	private static Logger logger = LoggerFactory.getLogger(KafkaNettyConnectHandler.class);
	
	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		logger.info("inboundChannel {} is registered", getInboundChannel(ctx.channel()));
		super.channelRegistered(ctx);
	}
	
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		logger.info("inboundChannel {} is unregistered", getInboundChannel(ctx.channel()));
		super.channelUnregistered(ctx);
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("inboundChannel {} is active", getInboundChannel(ctx.channel()));
		super.channelActive(ctx);
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		logger.info("inboundChannel {} is inactive", getInboundChannel(ctx.channel()));
		super.channelInactive(ctx);
	}
	
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent stateEvent = (IdleStateEvent) evt;
			if (stateEvent.state() == IdleState.ALL_IDLE) { ;
				logger.info("inboundChannel {} is idle", getInboundChannel(ctx.channel()));
				ChannelUtil.closeChannel(ctx.channel());
			}
		}
		ctx.fireUserEventTriggered(evt);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.info("inboundChannel {} occurs exception {}", getInboundChannel(ctx.channel()), cause);
		ChannelUtil.closeChannel(ctx.channel());
	}
}
