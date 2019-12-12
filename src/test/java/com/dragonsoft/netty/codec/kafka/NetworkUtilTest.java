package com.dragonsoft.netty.codec.kafka;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author: ronhunlam
 * date:2019/12/10 20:16
 */
public class NetworkUtilTest {
	
	@Test
	public void test_getLocalHostName() {
		String localName = NetworkUtil.getLocalHostName();
		Assert.assertNotNull(localName);
	}
	
	@Test
	public void test_getRealIpFromHostName() {
		String realIp = NetworkUtil.getRealIpFromHostName("localhost");
		Assert.assertEquals(realIp, "127.0.0.1");
	}
}
