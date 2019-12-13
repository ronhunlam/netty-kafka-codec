package com.dragonsoft.netty.codec.kafka;

import io.netty.util.internal.StringUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** network util
 * @author: ronhunlam
 * date:2019/12/6 11:45
 */
public class NetworkUtil {
	
	private NetworkUtil() {
	
	}
	/** get local host name.
	 * @param
	 * @return local host name
	 * @throws
	*/
	public static String getLocalHostName() {
		String localHostName = "";
		try {
			localHostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		return localHostName;
	}
	
	/** resolve hostname to ip
	 * @param hostName
	 * @return ip corresponding to the hostname
	 * @throws
	*/
	public static String getRealIpFromHostName(String hostName) {
		if (StringUtil.isNullOrEmpty(hostName)) {
			throw new IllegalArgumentException("The hostName can't be empty!");
		}
		try {
			InetAddress inetAddress = InetAddress.getByName(hostName);
			return inetAddress.getHostAddress();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}
}
