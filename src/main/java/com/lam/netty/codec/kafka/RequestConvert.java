package com.lam.netty.codec.kafka;

import java.nio.ByteBuffer;

/** convert {@link KafkaNettyRequest} to {@link ByteBuffer}
 * @author: ronhunlam
 * date:2019/12/6 15:39
 */
public interface RequestConvert {
	
	ByteBuffer convertRequestToBuffer(KafkaNettyRequest request);
}
