package com.dragonsoft.netty.codec.kafka;

import io.netty.channel.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.requests.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.dragonsoft.netty.codec.kafka.ChannelUtil.getInboundChannel;
import static org.apache.kafka.common.internals.Topic.isInternal;

/** This handler is just for demonstrating.
 * @author: ronhunlam
 * date:2019/8/3 15:17
 */
@Deprecated
@ChannelHandler.Sharable
public class KafkaNettyServerHandler extends SimpleChannelInboundHandler<KafkaNettyRequest> {
	private static final Logger logger = LoggerFactory.getLogger(KafkaNettyServerHandler.class);
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ctx.read();
	}
	
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, KafkaNettyRequest request) throws Exception {
		logger.info("inbound channel {} active ", getInboundChannel(ctx.channel()));
		RequestHeader header = request.getRequestHeader();
		if (header.apiKey() == ApiKeys.SASL_HANDSHAKE) {
			logger.warn("Don't support SASL!");
		} else {
			switch (header.apiKey()) {
				case PRODUCE:
					System.out.println(request.toString());
					AbstractResponse produceResponse = new ProduceResponse(new HashMap<>());
					ctx.writeAndFlush(request.buildResponse(produceResponse)).addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (future.isSuccess()) {
								ctx.read();
							}
						}
					});
					break;
				case FETCH:
					System.out.println(request.toString());
					AbstractResponse fetchResponse = new FetchResponse<BaseRecords>(Errors.NONE, new LinkedHashMap<TopicPartition, FetchResponse.PartitionData<BaseRecords>>(), 0, 1);
					ctx.writeAndFlush(request.buildResponse(fetchResponse)).addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (future.isSuccess()) {
								ctx.read();
							}
						}
					});
					break;
				case LIST_OFFSETS:
					System.out.println(request.toString());
					break;
				case METADATA:
					System.out.println(request.toString());
					List<MetadataResponse.TopicMetadata> topicMetadataList = new ArrayList<>();
					MetadataResponse.TopicMetadata metadata = new MetadataResponse.TopicMetadata(Errors.LEADER_NOT_AVAILABLE, "kafka-test", isInternal("kafka-test"),
						Collections.emptyList());
					metadata.authorizedOperations(5);
					topicMetadataList.add(metadata);
					List<Node> nodes = new ArrayList<>();
					nodes.add(new Node(1, "localhost", 9092));
					nodes.add(new Node(2, "localhost", 9093));
					AbstractResponse metadataResponse = MetadataResponse.prepareResponse(0, nodes, "123456789", 0, topicMetadataList, 0);
					ctx.writeAndFlush(request.buildResponse(metadataResponse)).addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (future.isSuccess()) {
								ctx.read();
							}
						}
					});
					break;
				case OFFSET_COMMIT:
					System.out.println(request.toString());
					break;
				case OFFSET_FETCH:
					System.out.println(request.toString());
					break;
				case FIND_COORDINATOR:
					System.out.println(request.toString());
					AbstractResponse findCoordinatorResponse = new FindCoordinatorResponse(new FindCoordinatorResponseData()
						.setErrorCode(Errors.NONE.code())
						.setErrorMessage(Errors.NONE.message())
						.setNodeId(1)
						.setHost("127.0.0.1")
						.setPort(9092)
						.setThrottleTimeMs(0));
					ctx.writeAndFlush(request.buildResponse(findCoordinatorResponse)).addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (future.isSuccess()) {
								ctx.read();
							}
						}
					});
					break;
				case JOIN_GROUP:
					System.out.println(request.toString());
					AbstractResponse joinGroupResponse = new JoinGroupResponse(
					new JoinGroupResponseData()
						.setThrottleTimeMs(0)
						.setErrorCode(Errors.NONE.code())
						.setGenerationId(1)
						// roundrobin range
						.setProtocolName("range")
						.setLeader("1")
						.setMemberId("1")
						.setMembers(Collections.emptyList()));
					ctx.writeAndFlush(request.buildResponse(joinGroupResponse)).addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (future.isSuccess()) {
								ctx.read();
							}
						}
					});
					break;
				case HEARTBEAT:
					System.out.println(request.toString());
					break;
				case LEAVE_GROUP:
					System.out.println(request.toString());
					break;
				case SYNC_GROUP:
					System.out.println(request.toString());
					AbstractResponse syncGroupResponse = new SyncGroupResponse(new SyncGroupResponseData()
						.setErrorCode(Errors.NONE.code())
						.setAssignment(new byte[]{})
						.setThrottleTimeMs(0));
					ctx.writeAndFlush(request.buildResponse(syncGroupResponse)).addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (future.isSuccess()) {
								ctx.read();
							}
						}
					});
					break;
				case API_VERSIONS:
					System.out.println(request.toString());
					AbstractResponse apiVersionsResponse = ApiVersionsResponse.apiVersionsResponse(0, (byte) 2);
					ctx.writeAndFlush(request.buildResponse(apiVersionsResponse)).addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (future.isSuccess()) {
								ctx.read();
							}
						}
					});
					break;
			}
		}
	}
}