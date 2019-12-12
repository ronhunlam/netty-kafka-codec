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
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.dragonsoft.netty.codec.kafka.ChannelUtil.parseChannelLocalAddr;
import static com.dragonsoft.netty.codec.kafka.ChannelUtil.parseChannelRemoteAddr;
import static com.dragonsoft.netty.codec.kafka.KafkaNettyProxyConfig.*;
import static com.dragonsoft.netty.codec.kafka.NetworkUtil.getRealIpFromHostName;
import static org.apache.kafka.common.protocol.ApiKeys.*;

/**
 * @author: ronhunlam
 * date:2019/8/19 16:16
 */

public class KafkaNettyProxyFrontendHandler extends ChannelInboundHandlerAdapter {
	
	private static Logger logger = LoggerFactory.getLogger(LOGGER_NAME);
	
	private final AtomicBoolean isFirstInitial = new AtomicBoolean(true);
	private final RequestConvert requestConvert;
	public static final AttributeKey<KafkaNettyRequest> requestKey = AttributeKey.newInstance("request");
	private final Queue<KafkaNettyRequest> cachedRequests;
	private Channel apiVersionChannel = null;
	private Channel newChannel = null;
	private final Set<Channel> channels = new HashSet<>();
	
	public KafkaNettyProxyFrontendHandler() {
		this(new DefaultRequestConvert(), 32);
	}
	
	public KafkaNettyProxyFrontendHandler(RequestConvert convert, int queueSize) {
		isFirstInitial.compareAndSet(true, false);
		this.requestConvert = convert;
		// maybe used for multi-thread.
		this.cachedRequests = new ArrayBlockingQueue<>(queueSize);
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("inbound channel active local : " + parseChannelLocalAddr(ctx.channel()) + " remote: " + parseChannelRemoteAddr(ctx.channel()));
		ctx.channel().read();
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		for (Channel channel : channels) {
			ChannelUtil.closeChannel(channel);
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("inbound channel exception: ", cause);
		ctx.channel().close();
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		if (msg != null && msg instanceof KafkaNettyRequest) {
			KafkaNettyRequest request = (KafkaNettyRequest) msg;
			ApiKeys apiKeys = request.getRequestBody().api;
			//ctx.channel().attr(requestKey).set(request);
			cachedRequests.offer(request);
			if (apiKeys == API_VERSIONS || apiKeys == METADATA || apiKeys == FIND_COORDINATOR) {
				apiVersionChannel = getOrOpenStaticOutboundChannel(ctx.channel(), request);
				channels.add(apiVersionChannel);
			} else if (apiKeys == PRODUCE) {
				channels.add(getOrOpenProduceChannelAndWrite(ctx.channel(), request));
			} else if (apiKeys == FETCH) {
				channels.add(getOrOpenFetchChannel(ctx.channel(), request));
			} else if (apiKeys == JOIN_GROUP || apiKeys == SYNC_GROUP
				|| apiKeys == HEARTBEAT || apiKeys == OFFSET_FETCH
				|| apiKeys == OFFSET_COMMIT) {
				channels.add(getOrOpenJoinGroupChannel(ctx.channel(), request));
			} else {
				logger.info("outbound channel write unknown request {} to channel {} ", request.toString(), parseChannelRemoteAddr(apiVersionChannel));
				/*ByteBuffer requestBuffer = requestConvert.convertRequestToBuffer(request);
				apiVersionChannel.writeAndFlush(requestBuffer).addListener((ChannelFutureListener) future -> {
					if (future.isSuccess()) {
						logger.info("outbound channel write request {} to channel {} successfully ", request.toString(), parseChannelRemoteAddr(apiVersionChannel));
						//addOriginRequest(ctx.channel().id().asShortText(), request);
						ctx.channel().read();
					} else {
						future.channel().close();
					}
				});*/
			}
		}
		ReferenceCountUtil.release(msg);
	}
	
	private Channel getOrOpenStaticOutboundChannel(Channel inboundChannel, KafkaNettyRequest request) {
		if (apiVersionChannel != null) {
			writeRequestToChannel(apiVersionChannel, request).addListener(future -> {
				inboundChannel.read();
			});
			return apiVersionChannel;
		}
		ChannelFuture cf = openOutboundChannel(inboundChannel, KAFKA_HOST, KAFKA_PORT);
		if (cf.isDone()) {
			writeRequestToChannel(cf.channel(), request).addListener(future -> {
				inboundChannel.read();
			});
		} else {
			cf.addListener(future -> {
				writeRequestToChannel(cf.channel(), request).addListener(future1 -> {
					inboundChannel.read();
				});
			});
		}
		return cf.channel();
	}
	
	private Channel getOrOpenProduceChannelAndWrite(Channel inboundChannel, KafkaNettyRequest request) {
		if (newChannel != null) {
			writeRequestToChannel(newChannel, request).addListener(future -> {
				if (future.isSuccess()) {
					inboundChannel.read();
				}
			});
			return newChannel;
		}
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
					Node leader = partitionMetadata.leader();
					NodeWrapper rawLeader = MetadataCache.getNodeInfo(leader.id());
					host = rawLeader.getHost();
					port = rawLeader.getPort();
				}
			}
		}
		if (!StringUtil.isNullOrEmpty(host) && port != 0) {
			String realIp = getRealIpFromHostName(host);
			if (!checkHostAndPortIsNew(realIp, port)) {
				writeRequestToChannel(apiVersionChannel, request).addListener(future -> {
					if (future.isSuccess()) {
						inboundChannel.read();
					}
				});
				return apiVersionChannel;
			} else {
				ChannelFuture channelFuture = openOutboundChannel(inboundChannel, realIp, port);
				newChannel = channelFuture.channel();
				channelFuture.addListener(future -> {
					if (future.isSuccess()) {
						writeRequestToChannel(newChannel, request).addListener(future1 -> {
							if (future1.isSuccess()) {
								inboundChannel.read();
							}
						});
					}
				});
				return newChannel;
			}
		}
		return apiVersionChannel;
	}
	
	private Channel getOrOpenJoinGroupChannel(Channel inboundChannel, KafkaNettyRequest request) {
		if (newChannel != null) {
			writeRequestToChannel(newChannel, request).addListener(new GenericFutureListener<Future<? super Void>>() {
				@Override
				public void operationComplete(Future<? super Void> future) throws Exception {
					if (future.isSuccess()) {
						inboundChannel.read();
					}
				}
			});
			return newChannel;
		}
		AbstractRequest requestBody = request.getRequestBody();
		String groupId = "";
		if (requestBody instanceof JoinGroupRequest) {
			groupId = ((JoinGroupRequest) requestBody).data().groupId();
		}
		if (requestBody instanceof SyncGroupRequest) {
			groupId = ((SyncGroupRequest) requestBody).data.groupId();
		}
		if (requestBody instanceof OffsetFetchRequest) {
			groupId = ((OffsetFetchRequest) requestBody).groupId();
		}
		if (requestBody instanceof OffsetCommitRequest) {
			groupId = ((OffsetCommitRequest) requestBody).data().groupId();
		}
		if (requestBody instanceof HeartbeatRequest) {
			groupId = ((HeartbeatRequest) requestBody).data.groupId();
		}
		NodeWrapper coordinator = CoordinatorCache.getCoordinator(groupId);
		String host = coordinator.getHost();
		int port = coordinator.getPort();
		String realIp = getRealIpFromHostName(host);
		if (!checkHostAndPortIsNew(realIp, port)) {
			writeRequestToChannel(apiVersionChannel, request).addListener(future -> {
				if (future.isSuccess()) {
					inboundChannel.read();
				}
			});
			return apiVersionChannel;
		}
		ChannelFuture channelFuture = openOutboundChannel(inboundChannel, realIp, port);
		newChannel = channelFuture.channel();
		channelFuture.addListener(future -> {
			if (future.isSuccess()) {
				writeRequestToChannel(newChannel, request).addListener(future1 -> {
					if (future1.isSuccess()) {
						inboundChannel.read();
					}
				});
			}
		});
		return newChannel;
	}
	
	private Channel getOrOpenFetchChannel(Channel inboundChannel, KafkaNettyRequest request) {
		if (newChannel != null) {
			writeRequestToChannel(newChannel, request).addListener(new GenericFutureListener<Future<? super Void>>() {
				@Override
				public void operationComplete(Future<? super Void> future) throws Exception {
					if (future.isSuccess()) {
						inboundChannel.read();
					}
				}
			});
			return newChannel;
		}
		FetchRequest fetchRequest = (FetchRequest) request.getRequestBody();
		Map<TopicPartition, FetchRequest.PartitionData> partitionDatas = fetchRequest.fetchData();
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
				if (partitionDatas.get(topicPartition) != null) {
					Node leader = partitionMetadata.leader();
					NodeWrapper rawLeader = MetadataCache.getNodeInfo(leader.id());
					host = rawLeader.getHost();
					port = rawLeader.getPort();
				}
			}
		}
		// go to the same node,so we use the same channel.
		if (StringUtils.isEmpty(host)) {
			writeRequestToChannel(apiVersionChannel, request).addListener(future -> {
				if (future.isSuccess()) {
					inboundChannel.read();
				}
			});
			return apiVersionChannel;
		}
		String realIp = getRealIpFromHostName(host);
		if (!checkHostAndPortIsNew(realIp, port)) {
			writeRequestToChannel(apiVersionChannel, request).addListener(future -> {
				if (future.isSuccess()) {
					inboundChannel.read();
				}
			});
			return apiVersionChannel;
		}
		ChannelFuture channelFuture = openOutboundChannel(inboundChannel, realIp, port);
		newChannel = channelFuture.channel();
		channelFuture.addListener(future -> {
			if (future.isSuccess()) {
				writeRequestToChannel(newChannel, request).addListener(future1 -> {
					if (future1.isSuccess()) {
						inboundChannel.read();
					}
				});
			}
		});
		return newChannel;
	}
	
	private boolean checkHostAndPortIsNew(String realIp, int port) {
		InetSocketAddress inetSocketAddress = (InetSocketAddress) apiVersionChannel.remoteAddress();
		String apiVersionHost = inetSocketAddress.getHostString();
		int apiVersionPort = inetSocketAddress.getPort();
		if (realIp.equals(apiVersionHost) && port == apiVersionPort) {
			return false;
		}
		return true;
	}
	
	/**
	 * open connection to the remote sever
	 *
	 * @param inboundChannel
	 * @param host
	 * @param port
	 * @return {@link ChannelFuture}
	 * @throws
	 */
	private ChannelFuture openOutboundChannel(Channel inboundChannel, String host, int port) {
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
						.addLast(new KafkaResponseDecoder(inboundChannel, cachedRequests))
						.addLast(new KafkaNettyProxyBackendHandler(inboundChannel));
				}
			});
		ChannelFuture f = client.connect(host, port);
		return f;
	}
	
	private ChannelFuture writeRequestToChannel(Channel outboundChannel, KafkaNettyRequest request) {
		ByteBuffer buffer = requestConvert.convertRequestToBuffer(request);
		return outboundChannel.writeAndFlush(buffer);
	}
}
