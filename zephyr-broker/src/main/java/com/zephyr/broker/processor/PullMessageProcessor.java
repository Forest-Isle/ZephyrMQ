package com.zephyr.broker.processor;

import com.zephyr.broker.ZephyrBroker;
import com.zephyr.protocol.message.MessageExt;
import com.zephyr.protocol.network.RemotingCommand;
import com.zephyr.protocol.network.RequestProcessor;
import com.zephyr.protocol.network.ResponseCode;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PullMessageProcessor implements RequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(PullMessageProcessor.class);

    private final ZephyrBroker broker;

    public PullMessageProcessor(ZephyrBroker broker) {
        this.broker = broker;
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, Object request, int requestId) {
        if (request instanceof RemotingCommand) {
            try {
                RemotingCommand response = processRequestInternal(ctx, (RemotingCommand) request);
                ctx.writeAndFlush(response);
            } catch (Exception e) {
                logger.error("Error processing pull message request", e);
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
        logger.debug("Processing pull message request: {}", request);

        try {
            // Parse pull request parameters
            PullMessageRequestHeader requestHeader = parsePullMessageRequest(request);

            // Validate request
            validatePullRequest(requestHeader);

            // Pull messages from store
            List<MessageExt> messages = broker.getMessageStore().getMessage(
                    requestHeader.getTopic(),
                    requestHeader.getQueueId(),
                    requestHeader.getQueueOffset(),
                    requestHeader.getMaxMsgNums()
            );

            // Create response
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "success");

            if (messages != null && !messages.isEmpty()) {
                // Serialize messages for response
                byte[] responseBody = serializeMessages(messages);
                response.setBody(responseBody);

                logger.debug("Pulled {} messages for topic={}, queueId={}, offset={}",
                        messages.size(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset());
            } else {
                response.setCode(ResponseCode.PULL_NOT_FOUND);
                response.setRemark("No new message");
                logger.debug("No messages found for topic={}, queueId={}, offset={}",
                        requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset());
            }

            return response;

        } catch (Exception e) {
            logger.error("Error processing pull message request", e);

            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ResponseCode.SYSTEM_ERROR, "Internal server error: " + e.getMessage());
            return response;
        }
    }

    private PullMessageRequestHeader parsePullMessageRequest(RemotingCommand request) {
        // Simple implementation - parse from request header
        PullMessageRequestHeader header = new PullMessageRequestHeader();
        header.setTopic("TestTopic"); // Default for now
        header.setQueueId(0);
        header.setQueueOffset(0);
        header.setMaxMsgNums(32);
        header.setConsumerGroup("DefaultConsumerGroup");
        return header;
    }

    private void validatePullRequest(PullMessageRequestHeader requestHeader) throws Exception {
        if (requestHeader.getTopic() == null || requestHeader.getTopic().trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }

        if (requestHeader.getQueueId() < 0) {
            throw new IllegalArgumentException("Queue ID cannot be negative");
        }

        if (requestHeader.getQueueOffset() < 0) {
            throw new IllegalArgumentException("Queue offset cannot be negative");
        }

        if (requestHeader.getMaxMsgNums() <= 0 || requestHeader.getMaxMsgNums() > 1000) {
            throw new IllegalArgumentException("Max message number must be between 1 and 1000");
        }
    }

    private byte[] serializeMessages(List<MessageExt> messages) {
        // Simple serialization - in real implementation would use efficient binary format
        StringBuilder sb = new StringBuilder();
        sb.append("messageCount=").append(messages.size()).append("\n");

        for (int i = 0; i < messages.size(); i++) {
            MessageExt message = messages.get(i);
            sb.append("message").append(i).append("=");
            sb.append("msgId:").append(message.getMsgId()).append(",");
            sb.append("topic:").append(message.getTopic()).append(",");
            sb.append("tags:").append(message.getTags()).append(",");
            sb.append("keys:").append(message.getKeys()).append(",");
            sb.append("body:").append(new String(message.getBody())).append(",");
            sb.append("queueId:").append(message.getQueueId()).append(",");
            sb.append("queueOffset:").append(message.getQueueOffset());
            sb.append("\n");
        }

        return sb.toString().getBytes();
    }

    public static class PullMessageRequestHeader {
        private String topic;
        private int queueId;
        private long queueOffset;
        private int maxMsgNums;
        private String consumerGroup;
        private long suspendTimeoutMillis = 15000; // 15s
        private long timeoutMillis = 10000; // 10s

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getQueueId() {
            return queueId;
        }

        public void setQueueId(int queueId) {
            this.queueId = queueId;
        }

        public long getQueueOffset() {
            return queueOffset;
        }

        public void setQueueOffset(long queueOffset) {
            this.queueOffset = queueOffset;
        }

        public int getMaxMsgNums() {
            return maxMsgNums;
        }

        public void setMaxMsgNums(int maxMsgNums) {
            this.maxMsgNums = maxMsgNums;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
        }

        public long getSuspendTimeoutMillis() {
            return suspendTimeoutMillis;
        }

        public void setSuspendTimeoutMillis(long suspendTimeoutMillis) {
            this.suspendTimeoutMillis = suspendTimeoutMillis;
        }

        public long getTimeoutMillis() {
            return timeoutMillis;
        }

        public void setTimeoutMillis(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
        }
    }
}