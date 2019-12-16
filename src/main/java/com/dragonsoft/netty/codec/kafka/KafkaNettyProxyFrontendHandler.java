package com.dragonsoft.netty.codec.kafka;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;
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
	
	private final RequestConvert requestConvert;
	private final Queue<KafkaNettyRequest> cachedRequests;
	private Channel apiVersionChannel = null;
	private Channel newChannel = null;
	private final Set<Channel> channels = new HashSet<>();
	
	public KafkaNettyProxyFrontendHandler() {
		this(new DefaultRequestConvert(), 1000);
	}
	
	public KafkaNettyProxyFrontendHandler(RequestConvert convert, int queueSize) {
		this.requestConvert = convert;
		// maybe used for multi-thread.
		this.cachedRequests = new ArrayBlockingQueue<>(queueSize);
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		logger.info("inbound channel local address {} remote address {} active", parseChannelLocalAddr(ctx.channel()),
			parseChannelRemoteAddr(ctx.channel()));
		ctx.channel().read();
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		logger.info("inbound channel local address {} remote address {} inactive", parseChannelLocalAddr(ctx.channel()),
			parseChannelRemoteAddr(ctx.channel()));
	}
	
	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		logger.info("inbound channel local address {} remote address {} unregister from selector", parseChannelLocalAddr(ctx.channel()),
			parseChannelRemoteAddr(ctx.channel()));
		clearCacheRequests();
		for (Channel channel : channels) {
			ChannelUtil.closeChannel(channel);
		}
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("inbound channel local address {} remote address {} exception {}", parseChannelLocalAddr(ctx.channel()),
			parseChannelRemoteAddr(ctx.channel()), cause);
		clearCacheRequests();
		ctx.channel().close();
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		if (msg != null && msg instanceof KafkaNettyRequest) {
			KafkaNettyRequest request = (KafkaNettyRequest) msg;
			logger.info("outbound channel local address {} remote address {} read request {}", parseChannelLocalAddr(ctx.channel()),
				parseChannelRemoteAddr(ctx.channel()), request);
			ApiKeys apiKeys = request.getRequestBody().api;
			cachedRequests.offer(request);
			if (apiKeys == API_VERSIONS || apiKeys == METADATA || apiKeys == FIND_COORDINATOR) {
				apiVersionChannel = getOrOpenStaticOutboundChannel(ctx.channel(), request);
				channels.add(apiVersionChannel);
			} else if (apiKeys == PRODUCE) {
				channels.add(getOrOpenProduceChannelAndWrite(ctx.channel(), request));
			} else if (apiKeys == FETCH) {
				channels.add(getOrOpenFetchChannel(ctx.channel(), request));
			} else if (apiKeys == LIST_OFFSETS) {
				channels.add(getOrOpenListOffsetChannel(ctx.channel(), request));
			} else if (apiKeys == JOIN_GROUP || apiKeys == SYNC_GROUP
				|| apiKeys == HEARTBEAT || apiKeys == OFFSET_FETCH
				|| apiKeys == OFFSET_COMMIT || apiKeys == LEAVE_GROUP) {
				channels.add(getOrOpenJoinGroupChannel(ctx.channel(), request));
			} else {
				logger.info("write remaining request {} to apiVersionChannel local address {} remote address {}",
					request.toString(), parseChannelLocalAddr(ctx.channel()), parseChannelRemoteAddr(apiVersionChannel));
				ByteBuffer requestBuffer = requestConvert.convertRequestToBuffer(request);
				apiVersionChannel.writeAndFlush(requestBuffer).addListener((ChannelFutureListener) future -> {
					if (future.isSuccess()) {
						ctx.channel().read();
					} else {
						future.channel().close();
					}
				});
			}
		}
		ReferenceCountUtil.release(msg);
	}
	
	/**
	 * Open fixed channel to get {@link ApiVersionsResponse} or {@link MetadataResponse} etc.
	 *
	 * @param inboundChannel
	 * @param request
	 * @return {@link Channel}
	 * @throws
	 */
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
	
	/**
	 * Open the channel to broker for writing {@link ProduceRequest}
	 *
	 * @param inboundChannel
	 * @param request
	 * @return {@link Channel}
	 * @throws
	 */
	private Channel getOrOpenProduceChannelAndWrite(Channel inboundChannel, KafkaNettyRequest request) {
		if (newChannel != null) {
			writeToNewChannelAndReadFromInboundChannel(request, inboundChannel);
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
				return openNewChannelAndWriteThenReadFromInboundChannel(channelFuture, request, inboundChannel);
			}
		}
		return apiVersionChannel;
	}
	
	/**
	 * Get or Open the fetch channel for {@link JoinGroupRequest}
	 *
	 * @param inboundChannel
	 * @param request
	 * @return {@link Channel}
	 * @throws
	 */
	private Channel getOrOpenJoinGroupChannel(Channel inboundChannel, KafkaNettyRequest request) {
		if (newChannel != null) {
			writeToNewChannelAndReadFromInboundChannel(request, inboundChannel);
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
		// convert hostname to real ip.
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
		return openNewChannelAndWriteThenReadFromInboundChannel(channelFuture, request, inboundChannel);
	}
	
	/**
	 * get or open the channel for {@link ListOffsetRequest}
	 *
	 * @param inboundChannel
	 * @param request
	 * @return {@link Channel}
	 * @throws
	 */
	private Channel getOrOpenListOffsetChannel(Channel inboundChannel, KafkaNettyRequest request) {
		if (newChannel != null) {
			writeToNewChannelAndReadFromInboundChannel(request, inboundChannel);
		}
		ListOffsetRequest listOffsetRequest = (ListOffsetRequest) request.getRequestBody();
		Map<TopicPartition, ListOffsetRequest.PartitionData> partitionRecords = listOffsetRequest.partitionTimestamps();
		MetadataResponse cache = MetadataCache.getCache();
		Collection<MetadataResponse.TopicMetadata> topicMetadatas = cache.topicMetadata();
		TopicPartition topicPartition;
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
		return openNewChannelAndWriteThenReadFromInboundChannel(channelFuture, request, inboundChannel);
	}
	
	/**
	 * Get or Open the fetch channel for {@link FetchRequest}
	 *
	 * @param inboundChannel
	 * @param request
	 * @return {@link Channel}
	 * @throws
	 */
	private Channel getOrOpenFetchChannel(Channel inboundChannel, KafkaNettyRequest request) {
		if (newChannel != null) {
			writeToNewChannelAndReadFromInboundChannel(request, inboundChannel);
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
		return openNewChannelAndWriteThenReadFromInboundChannel(channelFuture, request, inboundChannel);
	}
	
	/**
	 * Check the {@code realIp} and {@code port} are whether equal to the {@code apiVersionChannel}’s
	 * host and port respectively.
	 *
	 * @param realIp
	 * @param port
	 * @return {@code boolean}
	 * @throws
	 */
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
	
	/**
	 * Write {@link KafkaNettyRequest} to {@Code OutboundChannel}
	 *
	 * @param outboundChannel
	 * @param request
	 * @return {@link ChannelFuture}
	 * @throws
	 */
	private ChannelFuture writeRequestToChannel(Channel outboundChannel, KafkaNettyRequest request) {
		ByteBuffer buffer = requestConvert.convertRequestToBuffer(request);
		return outboundChannel.writeAndFlush(buffer);
	}
	
	/**
	 * write to new channel then read from inbound channel.
	 *
	 * @param request
	 * @param inboundChannel
	 * @return {@link Channel}
	 * @throws
	 */
	private Channel writeToNewChannelAndReadFromInboundChannel(KafkaNettyRequest request, Channel inboundChannel) {
		
		writeRequestToChannel(newChannel, request).addListener(future -> {
			if (future.isSuccess()) {
				inboundChannel.read();
			}
		});
		return newChannel;
	}
	
	/**
	 * open new channel and write then read from inbound channel.
	 *
	 * @param channelFuture
	 * @param request
	 * @param inboundChannel
	 * @return {@link Channel}
	 * @throws
	 */
	private Channel openNewChannelAndWriteThenReadFromInboundChannel(ChannelFuture channelFuture, KafkaNettyRequest request,
	                                                                 Channel inboundChannel) {
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
	
	/**
	 * clear the cache requests for avoiding OOM.
	 *
	 * @param
	 * @return
	 * @throws
	 */
	private void clearCacheRequests() {
		if (cachedRequests != null && !cachedRequests.isEmpty()) {
			Iterator<KafkaNettyRequest> requestIterator = cachedRequests.iterator();
			while (requestIterator.hasNext()) {
				KafkaNettyRequest request = requestIterator.next();
				logger.info("remove the cached request {}", request.toString());
				requestIterator.remove();
			}
		}
	}
}
