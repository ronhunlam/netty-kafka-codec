package com.lam.netty.codec.kafka;


import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

/**
 * @author: ronhunlam
 * date:2019/8/15 10:19
 */
public class ChannelUtil {
	
	private static Logger logger = LoggerFactory.getLogger(ChannelUtil.class.getName());
	
	private static String parseChannelRemoteAddr(final Channel channel) {
		if (null == channel) {
			return "";
		}
		SocketAddress remote = channel.remoteAddress();
		final String addr = remote != null ? remote.toString() : "";
		if (addr.length() > 0) {
			int slashIndex = addr.lastIndexOf("/");
			if (slashIndex >= 0) {
				return addr.substring(slashIndex + 1);
			}
			return addr;
		}
		return "";
	}
	
	private static String parseChannelLocalAddr(final Channel channel) {
		if (null == channel) {
			return "";
		}
		SocketAddress remote = channel.localAddress();
		final String addr = remote != null ? remote.toString() : "";
		if (addr.length() > 0) {
			int slashIndex = addr.lastIndexOf("/");
			if (slashIndex >= 0) {
				return addr.substring(slashIndex + 1);
			}
			return addr;
		}
		return "";
	}
	
	public static String getInboundChannel(Channel inbound) {
		if (inbound == null) {
			throw new IllegalArgumentException("inbound channel can't be null");
		}
		String inboundChannel = parseChannelRemoteAddr(inbound) + "-" + parseChannelLocalAddr(inbound);
		return inboundChannel;
	}
	
	public static String getOutboundChannel(Channel outbound) {
		if (outbound == null) {
			throw new IllegalArgumentException("outbound channel can't be null");
		}
		String outboundChannel = parseChannelLocalAddr(outbound) + "-" + parseChannelRemoteAddr(outbound);
		return outboundChannel;
	}
	
	
	public static void closeChannel(Channel channel) {
		final String localAddr = parseChannelLocalAddr(channel);
		final String remoteAddr = parseChannelRemoteAddr(channel);
		channel.close().addListener((ChannelFutureListener) channelFuture ->
			logger.info("close the channel local address {} to remote address {} result: {}", localAddr ,remoteAddr, channelFuture.isSuccess()));
	}
	
	public static void readIfNeeded(ChannelHandlerContext ctx) {
		if (!ctx.channel().config().isAutoRead()) {
			ctx.read();
		}
	}
}
