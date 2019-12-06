package com.dragonsoft.netty.codec.kafka;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author: ronhunlam
 * date:2019/12/6 11:45
 */
public class NetworkUtil {
	
	private NetworkUtil() {
	
	}
	
	public static String getLocalHostName() {
		String localHostName = "";
		try {
			localHostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		return localHostName;
	}
}
