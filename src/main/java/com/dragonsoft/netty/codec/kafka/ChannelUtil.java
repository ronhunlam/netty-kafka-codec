package com.dragonsoft.netty.codec.kafka;


import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

/**
 * @author: ronhunlam
 * date:2019/8/15 10:19
 */
public class ChannelUtil {
	
	private static Logger logger = LoggerFactory.getLogger(ChannelUtil.class.getName());
	
	public static String parseChannelRemoteAddr(final Channel channel) {
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
	
	public static void closeChannel(Channel channel) {
		final String remoteAddr = parseChannelRemoteAddr(channel);
		channel.close().addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture channelFuture) throws Exception {
				logger.info("close the channel to remote address[{}] result: {}", remoteAddr, channelFuture.isSuccess());
			}
		});
	}
}
