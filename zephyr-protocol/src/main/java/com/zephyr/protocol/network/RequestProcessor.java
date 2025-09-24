package com.zephyr.protocol.network;

import io.netty.channel.ChannelHandlerContext;

public interface RequestProcessor {

    void processRequest(ChannelHandlerContext ctx, Object request, int requestId);

    boolean rejectRequest();
}