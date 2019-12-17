package com.dragonsoft.netty.codec.kafka;

import io.netty.bootstrap.Bootstrap;

import java.nio.channels.Channel;

/** factory for client Bootstrap.
 * @author: ronhunlam
 * date:2019/12/17 14:42
 */
public interface ClientBootstrapFactory {
	Bootstrap newClientBootstrap(Channel inboundChannel);
}
