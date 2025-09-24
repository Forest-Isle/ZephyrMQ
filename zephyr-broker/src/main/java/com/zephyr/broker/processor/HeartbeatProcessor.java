package com.zephyr.broker.processor;

import com.zephyr.broker.ZephyrBroker;
import com.zephyr.protocol.network.RemotingCommand;
import com.zephyr.protocol.network.RequestProcessor;
import com.zephyr.protocol.network.ResponseCode;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatProcessor implements RequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(HeartbeatProcessor.class);

    private final ZephyrBroker broker;

    public HeartbeatProcessor(ZephyrBroker broker) {
        this.broker = broker;
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, Object request, int requestId) {
        if (request instanceof RemotingCommand) {
            try {
                RemotingCommand response = processRequestInternal(ctx, (RemotingCommand) request);
                ctx.writeAndFlush(response);
            } catch (Exception e) {
                logger.error("Error processing heartbeat request", e);
                RemotingCommand errorResponse = RemotingCommand.createResponseCommand(
                        ResponseCode.SYSTEM_ERROR, "Internal server error: " + e.getMessage());
                ctx.writeAndFlush(errorResponse);
            }
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequestInternal(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        logger.debug("Processing heartbeat request: {}", request);

        try {
            // Simple heartbeat response
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "heartbeat");

            logger.debug("Heartbeat processed successfully");
            return response;

        } catch (Exception e) {
            logger.error("Error processing heartbeat request", e);

            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ResponseCode.SYSTEM_ERROR, "Internal server error: " + e.getMessage());
            return response;
        }
    }
}