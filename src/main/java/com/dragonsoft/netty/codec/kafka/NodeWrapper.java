package com.dragonsoft.netty.codec.kafka;

/**
 * @author: ronhunlam
 * date:2019/12/10 22:49
 */
public class NodeWrapper {
	
	private int id;
	private String host;
	private int port;
	
	public NodeWrapper(int id, String host, int port) {
		this.id = id;
		this.host = host;
		this.port = port;
	}
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public String getHost() {
		return host;
	}
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public int getPort() {
		return port;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
}
