package com.doudou.server;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 上游 LLM 配置：App 只连本网关，网关再转发到这个地址。 */
@Component
@ConfigurationProperties(prefix = "doudou")
public class ProxyProperties {

    /** 上游 OpenAI 兼容服务的 Base URL，例如 http://192.168.1.6:11434/v1 */
    private String llmBaseUrl = "http://127.0.0.1:11434/v1";

    /** 连接上游的超时（秒）。流式读取不设总读超时，否则长回答会被掐断。 */
    private int connectTimeoutSeconds = 10;

    public String getLlmBaseUrl() {
        return llmBaseUrl;
    }

    public void setLlmBaseUrl(String llmBaseUrl) {
        this.llmBaseUrl = llmBaseUrl;
    }

    public int getConnectTimeoutSeconds() {
        return connectTimeoutSeconds;
    }

    public void setConnectTimeoutSeconds(int connectTimeoutSeconds) {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
    }

    /** 拼出 chat/completions 的完整地址，去掉末尾斜杠避免双斜杠。 */
    public String chatCompletionsUrl() {
        String base = llmBaseUrl == null ? "" : llmBaseUrl.trim();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + "/chat/completions";
    }
}
