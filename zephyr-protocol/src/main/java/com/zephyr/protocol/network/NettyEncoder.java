package com.zephyr.protocol.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

    private static final Logger logger = LoggerFactory.getLogger(NettyEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand msg, ByteBuf out) throws Exception {
        try {
            ProtocolUtils.encode(msg, out);
        } catch (Exception e) {
            logger.error("NettyEncoder encode error", e);
            ctx.close();
        }
    }
}