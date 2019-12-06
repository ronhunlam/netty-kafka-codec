package com.dragonsoft.netty.codec.kafka;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.LOGGER_NAME;
import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.localProxyPort;
import static com.dragonsoft.netty.codec.kafka.NetworkUtil.getLocalHostName;

/**
 * @author: ronhunlam
 * date:2019/8/19 16:29
 */
public class KafkaNettyProxyBackendHandler extends ChannelInboundHandlerAdapter {
	
	private static Logger logger = LoggerFactory.getLogger(LOGGER_NAME);
	private Channel inboundChannel;
	
	public KafkaNettyProxyBackendHandler(Channel inboundChannel) {
		this.inboundChannel = inboundChannel;
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("outbound channel active: " + ChannelUtil.parseChannelRemoteAddr(ctx.channel()));
		ctx.read();
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof KafkaNettyResponse) {
			KafkaNettyResponse response = (KafkaNettyResponse) msg;
			KafkaNettyRequest request = response.getRequest();
			AbstractResponse rawResponse = response.getResponseBody();
			ApiKeys requestApiKeys = request.getRequestHeader().apiKey();
			switch (requestApiKeys) {
				case PRODUCE:
					
					break;
				case FETCH:
					break;
				case METADATA:
					MetadataResponse mr = (MetadataResponse) rawResponse;
					MetadataResponseData mrp = mr.data();
					MetadataResponseData.MetadataResponseBrokerCollection mrdmrbc = mrp.brokers();
					for (MetadataResponseData.MetadataResponseBroker broker : mrdmrbc) {
						broker.setHost(InetAddress.getLocalHost().getHostName());
						broker.setPort(localProxyPort);
					}
					break;
				case FIND_COORDINATOR:
					modifyFindGroupResponse((FindCoordinatorResponse) rawResponse);
					break;
				case UPDATE_METADATA:
					break;
			}
			ByteBuffer resonseBuffer = rawResponse.serialize(request.apiVersion(), response.getResponseHeader());
			inboundChannel.writeAndFlush(resonseBuffer).addListener((ChannelFutureListener) future -> {
				if (future.isSuccess()) {
					ctx.channel().read();
					ctx.channel().config().setAutoRead(false);
				} else {
					future.channel().close();
				}
			});
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		ChannelUtil.closeChannel(inboundChannel);
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		ChannelUtil.closeChannel(inboundChannel);
	}
	
	private void modifyFindGroupResponse(FindCoordinatorResponse fr) {
		FindCoordinatorResponseData frData = fr.data();
		frData.setHost(getLocalHostName());
		frData.setPort(localProxyPort);
	}
}
