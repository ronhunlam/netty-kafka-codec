package com.dragonsoft.netty.codec.kafka;

import java.nio.ByteBuffer;

/**
 * @author: ronhunlam
 * date:2019/12/6 15:41
 */
public interface ResponseConvert {
	
	ByteBuffer convertResponseToBuffer(KafkaNettyResponse response);
}
