package com.dragonsoft.netty.codec.kafka;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.StringUtil;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.dragonsoft.netty.codec.kafka.ChannelUtil.parseChannelRemoteAddr;
import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.*;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;

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
	private final AtomicBoolean isInitialRead = new AtomicBoolean(true);
	private final RequestConvert requestConvert;
	public static final AttributeKey<KafkaNettyRequest> requestKey = AttributeKey.newInstance("request");
	
	public KafkaNettyProxyFrontendHandler(String host, int port) {
		this(host, port, new DefaultRequestConvert());
	}
	
	public KafkaNettyProxyFrontendHandler(String host, int port, RequestConvert convert) {
		this.host = host;
		this.port = port;
		this.requestConvert = convert;
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("inbound channel active local : " + ctx.channel().localAddress() + " remote: " + parseChannelRemoteAddr(ctx.channel()));
		ctx.channel().read();
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		outboundChannelId = "";
		ChannelUtil.closeChannel(outboundChannel);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error(cause.toString());
		outboundChannelId = "";
		ChannelUtil.closeChannel(outboundChannel);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		if (msg instanceof KafkaNettyRequest) {
			KafkaNettyRequest request = (KafkaNettyRequest) msg;
			ctx.channel().attr(requestKey).set(request);
			if (isInitialRead.compareAndSet(true, false)) {
				openOutboundChannelAccordingToMessageAndWrite(ctx, request);
			}
			if (outboundChannel != null) {
				if (request.getRequestBody().api == PRODUCE) {
					openProduceChannel(ctx.channel(), request);
					logger.info("{}", request);
				} else {
					ByteBuffer requestBuffer = requestConvert.convertRequestToBuffer(request);
					if (outboundChannel.isOpen() && outboundChannel.isActive() && outboundChannel.isWritable()) {
						outboundChannel.writeAndFlush(requestBuffer).addListener((ChannelFutureListener) future -> {
							if (future.isSuccess()) {
								logger.info("outbound channel write request {} to channel {} successfully ", request.toString(), parseChannelRemoteAddr(outboundChannel));
								//addOriginRequest(ctx.channel().id().asShortText(), request);
								ctx.channel().read();
							} else {
								future.channel().close();
							}
						});
					}
				}
			}
			ReferenceCountUtil.release(msg);
		}
	}
	
	private void openOutboundChannelAccordingToMessageAndWrite(ChannelHandlerContext inboundCtx, KafkaNettyRequest request) {
		AbstractRequest requestBody = request.getRequestBody();
		Channel inboundChannel = inboundCtx.channel();
		switch (requestBody.api) {
			case API_VERSIONS:
				openStaticOutboundChannel(inboundChannel, request);
				break;
			case PRODUCE:
				openProduceChannel(inboundChannel, request);
				break;
			case FETCH:
				break;
			case HEARTBEAT:
				HeartbeatRequest hr = (HeartbeatRequest) requestBody;
				openGroupCoordinatorChannel(inboundChannel, hr.data.groupId(), request);
				break;
			case OFFSET_FETCH:
				OffsetFetchRequest ofr = (OffsetFetchRequest) requestBody;
				openGroupCoordinatorChannel(inboundChannel, ofr.groupId(), request);
				break;
			case OFFSET_COMMIT:
				OffsetCommitRequest ocr = (OffsetCommitRequest) requestBody;
				openGroupCoordinatorChannel(inboundChannel, ocr.data().groupId(), request);
				break;
			case JOIN_GROUP:
				JoinGroupRequest jgr = (JoinGroupRequest) requestBody;
				openGroupCoordinatorChannel(inboundChannel, jgr.data().groupId(), request);
				break;
			case SYNC_GROUP:
				SyncGroupRequest sgr = (SyncGroupRequest) requestBody;
				openGroupCoordinatorChannel(inboundChannel, sgr.data.groupId(), request);
				break;
			default:
				break;
		}
	}
	
	private void openProduceChannel(Channel inboundChannel, KafkaNettyRequest request) {
		ProduceRequest produceRequest = (ProduceRequest) request.getRequestBody();
		Map<TopicPartition, MemoryRecords> partitionRecords = produceRequest.partitionRecordsOrFail();
		MetadataResponse cache = MetadataCache.getCache();
		Collection<MetadataResponse.TopicMetadata> topicMetadatas = cache.topicMetadata();
		TopicPartition topicPartition = null;
		String host = "";
		int port = 0;
		for (MetadataResponse.TopicMetadata topicMetadata : topicMetadatas) {
			Collection<MetadataResponse.PartitionMetadata> partitionMetadatas = topicMetadata.partitionMetadata();
			for (MetadataResponse.PartitionMetadata partitionMetadata : partitionMetadatas) {
				int partition = partitionMetadata.partition();
				topicPartition = new TopicPartition(topicMetadata.topic(), partition);
				if (partitionRecords.get(topicPartition) != null) {
					host = partitionMetadata.leader().host();
					port = partitionMetadata.leader().port();
				}
			}
		}
		if (!StringUtil.isNullOrEmpty(host) && port != 0) {
			openOutboundChannel(inboundChannel, host, port, request);
		}
	}
	
	private void openGroupCoordinatorChannel(Channel inboundChannel, String groupId, KafkaNettyRequest request) {
		Node coordinator = CoordinatorCache.getCoordinator(groupId).node();
		openOutboundChannel(inboundChannel, coordinator.host(), coordinator.port(), request);
	}
	
	private void openStaticOutboundChannel(Channel inboundChannel, KafkaNettyRequest request) {
		openOutboundChannel(inboundChannel, KAFKA_HOST, KAFKA_PORT, request);
	}
	
	private void openOutboundChannel(Channel inboundChannel, String host, int port, KafkaNettyRequest request) {
		Bootstrap client = new Bootstrap();
		client.group(inboundChannel.eventLoop())
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
						.addLast(new KafkaRequestEncoder(inboundChannel))
						.addLast(new KafkaResponseDecoder(inboundChannel))
						.addLast(new KafkaNettyProxyBackendHandler(inboundChannel));
				}
			});
		ChannelFuture f = client.connect(host, port);
		Channel oldOutboundChannel = outboundChannel;
		if (oldOutboundChannel != null) {
			oldOutboundChannel.close();
		}
		f.addListener(future -> {
			if (future.isSuccess()) {
				// inboundChannel.read();
				logger.info("outbound channel connect succeed : " + parseChannelRemoteAddr(outboundChannel));
				outboundChannel = f.channel();
				outboundChannelId = outboundChannel.id().asShortText();
				ByteBuffer requestBuffer = requestConvert.convertRequestToBuffer(request);
				outboundChannel.writeAndFlush(requestBuffer).addListener((ChannelFutureListener) writeFuture -> {
					if (future.isSuccess()) {
						inboundChannel.read();
						logger.info("outbound channel write request {} to channel {} successfully ", request.toString(), parseChannelRemoteAddr(outboundChannel));
					} else {
						writeFuture.channel().close();
					}
				});
			} else {
				inboundChannel.close();
				logger.info("outbound channel connect failed : " + parseChannelRemoteAddr(outboundChannel));
			}
		});
	}
}
