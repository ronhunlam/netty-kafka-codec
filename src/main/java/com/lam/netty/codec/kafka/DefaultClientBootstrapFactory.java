package com.lam.netty.codec.kafka;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;

import io.netty.channel.Channel;

/**
 * @author: ronhunlam
 * date:2019/12/17 14:46
 */
public class DefaultClientBootstrapFactory implements ClientBootstrapFactory {
	
	@Override
	public Bootstrap newClientBootstrap(Channel inboundChannel, ChannelInitializer channelInitializer) {
		Bootstrap client = new Bootstrap();
		client.group(inboundChannel.eventLoop())
			.option(ChannelOption.SO_KEEPALIVE, true)
			.option(ChannelOption.SO_BACKLOG, 1024)
			.option(ChannelOption.SO_RCVBUF, 65535)
			.option(ChannelOption.SO_SNDBUF, 65535)
			.option(ChannelOption.TCP_NODELAY, true)
			.option(ChannelOption.AUTO_READ, false)
			.channel(NioSocketChannel.class)
			.handler(channelInitializer);
		return client;
	}
}
