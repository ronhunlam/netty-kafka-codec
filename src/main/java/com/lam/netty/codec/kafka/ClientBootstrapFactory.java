package com.lam.netty.codec.kafka;

import io.netty.bootstrap.Bootstrap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

/** factory for client Bootstrap.
 * @author: ronhunlam
 * date:2019/12/17 14:42
 */
public interface ClientBootstrapFactory {
	Bootstrap newClientBootstrap(Channel inboundChannel, ChannelInitializer channelInitializer);
}
