package com.zephyr.protocol.network;

import com.zephyr.common.util.ZephyrThreadFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.zephyr.common.constant.ZephyrConstants.*;

public class NettyRemotingServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyRemotingServer.class);

    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final DefaultEventExecutorGroup defaultEventExecutorGroup;

    private final ConcurrentMap<Integer, RequestProcessor> processorTable = new ConcurrentHashMap<>();

    private final int port;
    private Channel serverChannel;

    public NettyRemotingServer(int port) {
        this.port = port;
        this.serverBootstrap = new ServerBootstrap();
        this.bossGroup = new NioEventLoopGroup(NETTY_BOSS_THREADS, new ZephyrThreadFactory("NettyBoss"));
        this.workerGroup = new NioEventLoopGroup(NETTY_WORKER_THREADS, new ZephyrThreadFactory("NettyWorker"));
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(NETTY_WORKER_THREADS, new ZephyrThreadFactory("NettyServerHandler"));
    }

    public void start() {
        ServerBootstrap bootstrap = this.serverBootstrap
                .group(this.bossGroup, this.workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, NETTY_SO_BACKLOG)
                .option(ChannelOption.SO_REUSEADDR, NETTY_SO_REUSEADDR)
                .childOption(ChannelOption.TCP_NODELAY, NETTY_TCP_NODELAY)
                .childOption(ChannelOption.SO_KEEPALIVE, NETTY_SO_KEEPALIVE)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(defaultEventExecutorGroup, "decoder", new NettyDecoder())
                                .addLast(defaultEventExecutorGroup, "encoder", new NettyEncoder())
                                .addLast(defaultEventExecutorGroup, "handler", new NettyServerHandler(NettyRemotingServer.this));
                    }
                });

        try {
            ChannelFuture future = bootstrap.bind(port).sync();
            this.serverChannel = future.channel();
            logger.info("Netty server started successfully on port {}", port);
        } catch (InterruptedException e) {
            logger.error("Failed to start netty server on port {}", port, e);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to start netty server", e);
        }
    }

    public void shutdown() {
        try {
            if (this.serverChannel != null) {
                this.serverChannel.close().sync();
            }
        } catch (InterruptedException e) {
            logger.error("Failed to shutdown server channel", e);
            Thread.currentThread().interrupt();
        }

        this.bossGroup.shutdownGracefully();
        this.workerGroup.shutdownGracefully();
        this.defaultEventExecutorGroup.shutdownGracefully();

        logger.info("Netty server shutdown successfully");
    }

    public void registerProcessor(int requestCode, RequestProcessor processor) {
        this.processorTable.put(requestCode, processor);
    }

    public RequestProcessor getProcessor(int requestCode) {
        return this.processorTable.get(requestCode);
    }
}