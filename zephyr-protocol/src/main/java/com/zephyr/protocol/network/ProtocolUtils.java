package com.zephyr.protocol.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ProtocolUtils {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolUtils.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static RemotingCommand decode(ByteBuf byteBuffer) {
        int length = byteBuffer.readableBytes();
        int oriHeaderLen = byteBuffer.readInt();
        int headerLength = getHeaderLength(oriHeaderLen);

        byte[] headerData = new byte[headerLength];
        byteBuffer.readBytes(headerData);

        RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));

        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.readBytes(bodyData);
        }
        cmd.setBody(bodyData);

        return cmd;
    }

    public static void encode(RemotingCommand remotingCommand, ByteBuf out) {
        try {
            byte[] headerData = headerEncode(remotingCommand);
            byte[] bodyData = remotingCommand.getBody();

            int bodyLength = bodyData != null ? bodyData.length : 0;
            int totalLength = 4 + headerData.length + bodyLength;

            out.writeInt(totalLength - 4);
            out.writeInt(makeHeaderLength(headerData.length));
            out.writeBytes(headerData);

            if (bodyData != null) {
                out.writeBytes(bodyData);
            }
        } catch (Exception e) {
            logger.error("encode error", e);
            throw new RuntimeException("encode error", e);
        }
    }

    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case JSON:
                return jsonDecode(headerData);
            default:
                throw new RuntimeException("Unknown serialize type: " + type);
        }
    }

    private static byte[] headerEncode(RemotingCommand remotingCommand) {
        return jsonEncode(remotingCommand);
    }

    private static RemotingCommand jsonDecode(byte[] headerData) {
        try {
            String json = new String(headerData, StandardCharsets.UTF_8);
            return objectMapper.readValue(json, RemotingCommand.class);
        } catch (Exception e) {
            logger.error("jsonDecode error", e);
            throw new RuntimeException("jsonDecode error", e);
        }
    }

    private static byte[] jsonEncode(RemotingCommand remotingCommand) {
        try {
            String json = objectMapper.writeValueAsString(remotingCommand);
            return json.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.error("jsonEncode error", e);
            throw new RuntimeException("jsonEncode error", e);
        }
    }

    private static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    private static SerializeType getProtocolType(int source) {
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    private static int makeHeaderLength(int length) {
        return length | (SerializeType.JSON.getCode() << 24);
    }

    private enum SerializeType {
        JSON((byte) 0),
        ROCKETMQ((byte) 1);

        private final byte code;

        SerializeType(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static SerializeType valueOf(byte code) {
            for (SerializeType serializeType : SerializeType.values()) {
                if (serializeType.getCode() == code) {
                    return serializeType;
                }
            }
            return null;
        }
    }
}