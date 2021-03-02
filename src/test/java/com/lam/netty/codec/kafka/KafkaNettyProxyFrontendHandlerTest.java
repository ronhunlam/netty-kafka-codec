package com.lam.netty.codec.kafka;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: ronhunlam
 * date:2019/12/13 11:23
 */
public class KafkaNettyProxyFrontendHandlerTest {

	@Test
	public void testReadingKafkaMessages() {
		EmbeddedChannel embeddedChannel = new EmbeddedChannel(new KafkaRequestDecoder(),
			new KafkaNettyProxyFrontendHandler());
		embeddedChannel.writeInbound("I am a talent!");
		Assert.assertTrue(embeddedChannel.readInbound() != null);
	}
}
