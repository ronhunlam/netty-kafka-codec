package com.dragonsoft.netty.codec.kafka;

import io.netty.bootstrap.Bootstrap;

import java.nio.channels.Channel;

/**
 * @author: ronhunlam
 * date:2019/12/17 14:46
 */
public class DefaultClientBootstrapFactory implements ClientBootstrapFactory {
	
	@Override
	public Bootstrap newClientBootstrap(Channel inboundChannel) {
		return null;
	}
}
