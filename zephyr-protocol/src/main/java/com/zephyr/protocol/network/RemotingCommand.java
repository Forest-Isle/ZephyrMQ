package com.zephyr.protocol.network;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RemotingCommand {

    private static final AtomicInteger REQUEST_ID = new AtomicInteger(0);

    @JsonProperty("code")
    private int code;

    @JsonProperty("language")
    private LanguageCode language = LanguageCode.JAVA;

    @JsonProperty("version")
    private int version = 1;

    @JsonProperty("opaque")
    private int opaque = REQUEST_ID.getAndIncrement();

    @JsonProperty("flag")
    private int flag = 0;

    @JsonProperty("remark")
    private String remark;

    @JsonProperty("extFields")
    private Map<String, String> extFields;

    private transient byte[] body;
    private transient Object customHeader;

    public static RemotingCommand createRequestCommand(int code) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.markRequestType();
        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRemark(remark);
        cmd.markResponseType();
        return cmd;
    }

    public void markRequestType() {
        int bits = 1 << 0;
        this.flag |= bits;
    }

    public void markResponseType() {
        int bits = 1 << 0;
        this.flag &= ~bits;
    }

    public void markOnewayRPC() {
        int bits = 1 << 1; // Use second bit for oneway flag
        this.flag |= bits;
    }

    public boolean isOnewayRPC() {
        int bits = 1 << 1;
        return (this.flag & bits) == bits;
    }

    public boolean isRequestType() {
        int bits = 1 << 0;
        return (this.flag & bits) == bits;
    }

    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<>();
        }
        extFields.put(key, value);
    }

    public String getExtField(String key) {
        if (null != extFields) {
            return extFields.get(key);
        }
        return null;
    }

    // Getters and Setters
    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getRequestId() {
        return this.opaque;
    }

    public void setRequestId(int requestId) {
        this.opaque = requestId;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Map<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(Map<String, String> extFields) {
        this.extFields = extFields;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Object getCustomHeader() {
        return customHeader;
    }

    public void setCustomHeader(Object customHeader) {
        this.customHeader = customHeader;
    }

    @Override
    public String toString() {
        return "RemotingCommand{" +
                "code=" + code +
                ", language=" + language +
                ", version=" + version +
                ", opaque=" + opaque +
                ", flag=" + flag +
                ", remark='" + remark + '\'' +
                ", extFields=" + extFields +
                '}';
    }
}