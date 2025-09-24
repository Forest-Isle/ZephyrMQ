package com.zephyr.protocol.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {

    private static final Logger logger = LoggerFactory.getLogger(NettyDecoder.class);

    private static final int FRAME_MAX_LENGTH = 16777216; // 16MB

    public NettyDecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }

            return ProtocolUtils.decode(frame);
        } catch (Exception e) {
            logger.error("NettyDecoder decode error", e);
            ctx.close();
        } finally {
            if (frame != null) {
                frame.release();
            }
        }
        return null;
    }
}