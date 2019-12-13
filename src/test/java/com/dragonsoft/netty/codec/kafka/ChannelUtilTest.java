package com.dragonsoft.netty.codec.kafka;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.netty.channel.ChannelFutureListener.CLOSE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author: ronhunlam
 * date:2019/12/13 10:21
 */
public class ChannelUtilTest {
	
	@Test
	public void test_closeChannel() {
		Channel channel = mock(Channel.class);
		when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 9092));
		when(channel.localAddress()).thenReturn(new InetSocketAddress("localhost", 7000));
		when(channel.close()).thenReturn(new ChannelFuture() {
			@Override
			public Channel channel() {
				return channel;
			}
			
			@Override
			public ChannelFuture addListener(GenericFutureListener<? extends Future<? super Void>> genericFutureListener) {
				if (genericFutureListener instanceof ChannelFutureListener) {
					try {
						((ChannelFutureListener)genericFutureListener).operationComplete(this);
					} catch (Exception e) {
						// ignore
					}
				}
				return this;
			}
			
			@Override
			public ChannelFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... genericFutureListeners) {
				return this;
			}
			
			@Override
			public ChannelFuture removeListener(GenericFutureListener<? extends Future<? super Void>> genericFutureListener) {
				return this;
			}
			
			@Override
			public ChannelFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... genericFutureListeners) {
				return this;
			}
			
			@Override
			public ChannelFuture sync() throws InterruptedException {
				return this;
			}
			
			@Override
			public ChannelFuture syncUninterruptibly() {
				return this;
			}
			
			@Override
			public ChannelFuture await() throws InterruptedException {
				return this;
			}
			
			@Override
			public ChannelFuture awaitUninterruptibly() {
				return this;
			}
			
			@Override
			public boolean isVoid() {
				return false;
			}
			
			@Override
			public boolean isSuccess() {
				return false;
			}
			
			@Override
			public boolean isCancellable() {
				return false;
			}
			
			@Override
			public Throwable cause() {
				return null;
			}
			
			@Override
			public boolean await(long l, TimeUnit timeUnit) throws InterruptedException {
				return false;
			}
			
			@Override
			public boolean await(long l) throws InterruptedException {
				return false;
			}
			
			@Override
			public boolean awaitUninterruptibly(long l, TimeUnit timeUnit) {
				return false;
			}
			
			@Override
			public boolean awaitUninterruptibly(long l) {
				return false;
			}
			
			@Override
			public Void getNow() {
				return null;
			}
			
			@Override
			public boolean cancel(boolean b) {
				return false;
			}
			
			@Override
			public boolean isCancelled() {
				return false;
			}
			
			@Override
			public boolean isDone() {
				return true;
			}
			
			@Override
			public Void get() throws InterruptedException, ExecutionException {
				return null;
			}
			
			@Override
			public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
				return null;
			}
		});
		ChannelUtil.closeChannel(channel);
	}
	
}
