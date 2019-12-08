package com.dragonsoft.netty.codec.kafka;

import java.nio.ByteBuffer;

/**
 * @author: ronhunlam
 * date:2019/12/6 15:39
 */
public interface RequestConvert {
	
	ByteBuffer convertRequestToBuffer(KafkaNettyRequest request);
}
