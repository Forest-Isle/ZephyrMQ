package com.zephyr.protocol.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyRemotingClient {

    private static final Logger logger = LoggerFactory.getLogger(NettyRemotingClient.class);

    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroup;
    private final ConcurrentMap<String, Channel> channelTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, CompletableFuture<RemotingCommand>> responseTable = new ConcurrentHashMap<>();
    private final AtomicInteger requestIdGenerator = new AtomicInteger(0);

    public NettyRemotingClient() {
        this.eventLoopGroup = new NioEventLoopGroup(1);
        this.bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new NettyEncoder())
                                .addLast(new NettyDecoder())
                                .addLast(new NettyClientHandler());
                    }
                });
    }

    public void start() {
        logger.info("NettyRemotingClient started");
    }

    public void shutdown() {
        logger.info("NettyRemotingClient shutting down...");

        // Close all channels
        for (Channel channel : channelTable.values()) {
            if (channel != null && channel.isActive()) {
                channel.close();
            }
        }

        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }

        logger.info("NettyRemotingClient shut down successfully");
    }

    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        Channel channel = getChannel(addr);
        if (channel != null && channel.isActive()) {
            int requestId = requestIdGenerator.incrementAndGet();
            request.setRequestId(requestId);

            CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
            responseTable.put(requestId, future);

            try {
                channel.writeAndFlush(request).addListener((ChannelFutureListener) channelFuture -> {
                    if (!channelFuture.isSuccess()) {
                        responseTable.remove(requestId);
                        future.completeExceptionally(channelFuture.cause());
                        logger.error("Send request failed, channel: {}", addr, channelFuture.cause());
                    }
                });

                return future.get(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                responseTable.remove(requestId);
                throw e;
            }
        } else {
            throw new Exception("Channel is not active for address: " + addr);
        }
    }

    public void invokeAsync(String addr, RemotingCommand request, InvokeCallback callback) {
        Channel channel = getChannel(addr);
        if (channel != null && channel.isActive()) {
            int requestId = requestIdGenerator.incrementAndGet();
            request.setRequestId(requestId);

            CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
            responseTable.put(requestId, future);

            future.whenComplete((response, throwable) -> {
                responseTable.remove(requestId);
                if (callback != null) {
                    if (throwable != null) {
                        callback.operationFailed(throwable);
                    } else {
                        callback.operationComplete(response);
                    }
                }
            });

            channel.writeAndFlush(request).addListener((ChannelFutureListener) channelFuture -> {
                if (!channelFuture.isSuccess()) {
                    future.completeExceptionally(channelFuture.cause());
                    logger.error("Send async request failed, channel: {}", addr, channelFuture.cause());
                }
            });
        } else {
            if (callback != null) {
                callback.operationFailed(new Exception("Channel is not active for address: " + addr));
            }
        }
    }

    public void invokeOneway(String addr, RemotingCommand request) {
        Channel channel = getChannel(addr);
        if (channel != null && channel.isActive()) {
            request.markOnewayRPC();
            channel.writeAndFlush(request).addListener((ChannelFutureListener) channelFuture -> {
                if (!channelFuture.isSuccess()) {
                    logger.error("Send oneway request failed, channel: {}", addr, channelFuture.cause());
                }
            });
        } else {
            logger.warn("Channel is not active for address: {}", addr);
        }
    }

    private Channel getChannel(String addr) {
        Channel channel = channelTable.get(addr);
        if (channel != null && channel.isActive()) {
            return channel;
        }

        return createChannel(addr);
    }

    private Channel createChannel(String addr) {
        try {
            String[] hostPort = addr.split(":");
            String host = hostPort[0];
            int port = Integer.parseInt(hostPort[1]);

            ChannelFuture channelFuture = bootstrap.connect(new InetSocketAddress(host, port)).sync();
            Channel channel = channelFuture.channel();

            channelTable.put(addr, channel);

            channel.closeFuture().addListener((ChannelFutureListener) future -> {
                channelTable.remove(addr);
                logger.info("Channel closed: {}", addr);
            });

            logger.info("Created channel to: {}", addr);
            return channel;

        } catch (Exception e) {
            logger.error("Failed to create channel to: {}", addr, e);
            return null;
        }
    }

    private class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand response) {
            int requestId = response.getRequestId();
            CompletableFuture<RemotingCommand> future = responseTable.remove(requestId);
            if (future != null) {
                future.complete(response);
            } else {
                logger.warn("Received response for unknown request: {}", requestId);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            logger.info("Channel inactive: {}", ctx.channel().remoteAddress());
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Exception caught in client handler", cause);
            ctx.close();
        }
    }

    public interface InvokeCallback {
        void operationComplete(RemotingCommand response);
        void operationFailed(Throwable throwable);
    }
}