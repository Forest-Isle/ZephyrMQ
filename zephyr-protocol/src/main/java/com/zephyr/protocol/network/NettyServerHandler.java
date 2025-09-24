package com.zephyr.protocol.network;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

    private static final Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);

    private final NettyRemotingServer remotingServer;

    public NettyServerHandler(NettyRemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
        processMessageReceived(ctx, msg);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("Channel active: {}", ctx.channel().remoteAddress());
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        logger.info("Channel inactive: {}", ctx.channel().remoteAddress());
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("NettyServerHandler exception caught", cause);
        ctx.close();
    }

    private void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand cmd) {
        if (cmd != null) {
            RequestProcessor processor = remotingServer.getProcessor(cmd.getCode());
            if (processor != null) {
                if (processor.rejectRequest()) {
                    logger.warn("Request rejected, code: {}", cmd.getCode());
                    return;
                }
                processor.processRequest(ctx, cmd, cmd.getOpaque());
            } else {
                logger.warn("No processor found for request code: {}", cmd.getCode());
                RemotingCommand response = RemotingCommand.createResponseCommand(
                        ResponseCode.REQUEST_CODE_NOT_SUPPORTED, "No processor found for request code: " + cmd.getCode()
                );
                response.setOpaque(cmd.getOpaque());
                ctx.writeAndFlush(response);
            }
        }
    }
}