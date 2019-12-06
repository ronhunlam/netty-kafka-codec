package com.dragonsoft.netty.codec.kafka;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.LOGGER_NAME;
import static com.dragonsoft.netty.codec.kafka.OriginRequestBuffer.addOriginRequest;

/**
 * @author: ronhunlam
 * date:2019/8/19 16:16
 */

public class KafkaNettyProxyFrontendHandler extends ChannelInboundHandlerAdapter {
	
	private static Logger logger = LoggerFactory.getLogger(LOGGER_NAME);
	
	private Channel outboundChannel;
	private String outboundChannelId;
	private final String host;
	private final int port;
	
	public KafkaNettyProxyFrontendHandler(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("inbound channel active local : " + ctx.channel().localAddress() + " remote: " + ChannelUtil.parseChannelRemoteAddr(ctx.channel()));
		final Channel inboundChannel = ctx.channel();
		Bootstrap client = new Bootstrap();
		client.group(ctx.channel().eventLoop())
			.option(ChannelOption.SO_KEEPALIVE, true)
			.option(ChannelOption.SO_BACKLOG, 1024)
			.option(ChannelOption.SO_RCVBUF, 65535)
			.option(ChannelOption.SO_SNDBUF, 65535)
			.option(ChannelOption.TCP_NODELAY, true)
			.option(ChannelOption.AUTO_READ, false)
			.channel(NioSocketChannel.class)
			.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel socketChannel) throws Exception {
					socketChannel.pipeline()
						.addLast(new KafkaRequestEncoder())
						.addLast(new KafkaResponseDecoder())
						.addLast(new KafkaNettyProxyBackendHandler(inboundChannel));
				}
			});
		ChannelFuture f = client.connect(host, port);
		outboundChannel = f.channel();
		outboundChannelId = outboundChannel.id().asShortText();
		f.addListener((ChannelFutureListener) channelFuture -> {
			if (channelFuture.isSuccess()) {
				inboundChannel.read();
				logger.info("outbound channel connect succeed : " + ChannelUtil.parseChannelRemoteAddr(outboundChannel));
			} else {
				inboundChannel.close();
			}
		});
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		outboundChannelId = "";
		ChannelUtil.closeChannel(outboundChannel);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		outboundChannelId = "";
		ChannelUtil.closeChannel(outboundChannel);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (outboundChannel.isOpen() && outboundChannel.isActive() && outboundChannel.isWritable()) {
			if (msg instanceof KafkaNettyRequest) {
				KafkaNettyRequest request = (KafkaNettyRequest) msg;
				RequestHeader header = request.getRequestHeader();
				AbstractRequest requestBody = request.getRequestBody();
				ByteBuffer requestBuffer = requestBody.serialize(header);
				outboundChannel.writeAndFlush(requestBuffer).addListener((ChannelFutureListener) future -> {
					if (future.isSuccess()) {
						addOriginRequest(outboundChannelId, request);
						logger.info("outbound channel write request : " + request.toString());
						logger.info("outbound channel write successfully : " + ChannelUtil.parseChannelRemoteAddr(outboundChannel));
						ctx.channel().read();
					} else {
						future.channel().close();
					}
				});
			}
		}
		ReferenceCountUtil.release(msg);
	}
	
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
	
	}
}
