package com.zephyr.broker.processor;

import com.zephyr.broker.ZephyrBroker;
import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageExt;
import com.zephyr.protocol.message.SendResult;
import com.zephyr.protocol.message.SendStatus;
import com.zephyr.protocol.network.RemotingCommand;
import com.zephyr.protocol.network.RequestProcessor;
import com.zephyr.protocol.network.ResponseCode;
import io.netty.channel.ChannelHandlerContext;
import com.zephyr.storage.commitlog.CommitLog.PutMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SendMessageProcessor implements RequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(SendMessageProcessor.class);

    private final ZephyrBroker broker;

    public SendMessageProcessor(ZephyrBroker broker) {
        this.broker = broker;
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, Object request, int requestId) {
        // Convert object to RemotingCommand and delegate to internal method
        if (request instanceof RemotingCommand) {
            try {
                RemotingCommand response = processRequestInternal(ctx, (RemotingCommand) request);
                // Send response back through channel
                ctx.writeAndFlush(response);
            } catch (Exception e) {
                logger.error("Error processing send message request", e);
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
        logger.debug("Processing send message request: {}", request);

        try {
            // Parse message from request
            Message message = parseMessageFromRequest(request);

            // Validate message
            validateMessage(message);

            // Create MessageExt for storage
            MessageExt messageExt = createMessageExt(message);

            // Store message
            PutMessageResult result = broker.getMessageStore().putMessage(messageExt);
            boolean storeOK = result.isOk();

            // Create response
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "success");

            if (storeOK) {
                SendResult sendResult = new SendResult();
                sendResult.setSendStatus(SendStatus.SEND_OK);
                sendResult.setMsgId(messageExt.getMsgId());
                sendResult.setMessageQueue(messageExt.getMessageQueue());
                sendResult.setQueueOffset(messageExt.getQueueOffset());

                response.setBody(serializeSendResult(sendResult));
                logger.debug("Message sent successfully: msgId={}, topic={}",
                        messageExt.getMsgId(), messageExt.getTopic());
            } else {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("Store message failed");
                logger.warn("Failed to store message: topic={}, body size={}",
                        message.getTopic(), message.getBody() != null ? message.getBody().length : 0);
            }

            return response;

        } catch (Exception e) {
            logger.error("Error processing send message request", e);

            RemotingCommand response = RemotingCommand.createResponseCommand(
                    ResponseCode.SYSTEM_ERROR, "Internal server error: " + e.getMessage());
            return response;
        }
    }

    private Message parseMessageFromRequest(RemotingCommand request) throws Exception {
        // Simple implementation - in real scenario, this would parse from request body
        // For now, create a mock message
        Message message = new Message();
        message.setTopic("TestTopic");
        message.setTags("TagA");
        message.setKeys("TestKey");
        message.setBody("Test message body".getBytes());
        return message;
    }

    private void validateMessage(Message message) throws Exception {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        if (message.getTopic() == null || message.getTopic().trim().isEmpty()) {
            throw new IllegalArgumentException("Message topic cannot be null or empty");
        }

        if (message.getBody() == null || message.getBody().length == 0) {
            throw new IllegalArgumentException("Message body cannot be null or empty");
        }

        if (message.getBody().length > broker.getBrokerConfig().getMaxMessageSize()) {
            throw new IllegalArgumentException("Message body size exceeds maximum allowed size");
        }
    }

    private MessageExt createMessageExt(Message message) {
        MessageExt messageExt = new MessageExt();

        // Copy basic properties
        messageExt.setTopic(message.getTopic());
        messageExt.setTags(message.getTags());
        messageExt.setKeys(message.getKeys());
        messageExt.setBody(message.getBody());
        messageExt.setFlag(message.getFlag());
        messageExt.setProperties(message.getProperties());

        // Set extended properties
        messageExt.setMsgId(generateMessageId());
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setStoreTimestamp(System.currentTimeMillis());
        messageExt.setBornHost(MessageExt.socketAddress2ByteBuffer("127.0.0.1:9876"));
        messageExt.setStoreHost(MessageExt.socketAddress2ByteBuffer("127.0.0.1:9876"));

        // Set queue information (simplified - normally would be calculated based on routing)
        messageExt.setQueueId(0);
        messageExt.setQueueOffset(System.currentTimeMillis()); // Simplified offset

        return messageExt;
    }

    private String generateMessageId() {
        return UUID.randomUUID().toString().replace("-", "").toUpperCase();
    }

    private byte[] serializeSendResult(SendResult sendResult) {
        // Simple serialization - in real implementation would use protobuf or json
        StringBuilder sb = new StringBuilder();
        sb.append("sendStatus=").append(sendResult.getSendStatus()).append(";");
        sb.append("msgId=").append(sendResult.getMsgId()).append(";");
        sb.append("queueOffset=").append(sendResult.getQueueOffset());
        return sb.toString().getBytes();
    }
}