package com.dragonsoft.netty.codec.kafka;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.*;

/**
 * @author: ronhunlam
 * date:2019/8/2 17:37
 */
public class KafkaNettyProxyServer implements ProxyServer {
	
	private ServerBootstrap serverBootstrap;
	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;
	private Channel channel;
	
	public KafkaNettyProxyServer() {
		serverBootstrap = new ServerBootstrap();
		bossGroup = new NioEventLoopGroup(1);
		workerGroup = new NioEventLoopGroup(8);
		serverBootstrap
			.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			.option(ChannelOption.SO_BACKLOG, 1024)
			.option(ChannelOption.SO_KEEPALIVE, true)
			.option(ChannelOption.TCP_NODELAY, true)
			.option(ChannelOption.SO_REUSEADDR, true)
			.option(ChannelOption.SO_RCVBUF, 65535)
			.option(ChannelOption.SO_SNDBUF, 65535)
			.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
			.localAddress(localProxyPort)
			.childOption(ChannelOption.AUTO_READ, false)
			.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel socketChannel) throws Exception {
					
					socketChannel.pipeline()
						.addLast(new KafkaResponseEncoder())
						.addLast(new KafkaRequestDecoder())
						/*.addLast(new IdleStateHandler(0, 0, CHANNEL_MAXIDLE_TIMESECONDS))*/
						/*.addLast(new KafkaNettyConnectHandler())*/
						/*	.addLast(new KafkaNettyServerHandler());*/
						.addLast(new KafkaNettyProxyFrontendHandler());
				}
			});
	}
	
	@Override
	public void start() {
		channel = serverBootstrap.bind().syncUninterruptibly().channel();
	}
	
	@Override
	public void shutdown() {
		if (channel != null) {
			channel.close();
		}
		if (bossGroup != null) {
			bossGroup.shutdownGracefully();
		}
		if (workerGroup != null) {
			workerGroup.shutdownGracefully();
		}
	}
}
