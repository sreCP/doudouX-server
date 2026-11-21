package com.doudou.server;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.nio.charset.StandardCharsets;

/**
 * OpenAI 兼容的 LLM 代理端点（无 /api 前缀，方便 App 直接把 Base URL 指到 /v1）。
 *
 * <p>App 侧 Base URL 填 http://<网关IP>:8080/v1，引擎会拼成
 * http://<网关IP>:8080/v1/chat/completions，正好命中这里。
 */
@RestController
public class LlmProxyController {

    private final LlmProxyService proxy;

    public LlmProxyController(LlmProxyService proxy) {
        this.proxy = proxy;
    }

    /** 流式聊天：逐行转发上游 SSE，同时自动记一条 trace。 */
    @PostMapping(value = "/v1/chat/completions", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public StreamingResponseBody chat(@RequestBody byte[] body) {
        return proxy.proxy(body);
    }

    /** 模型列表占位，避免 App 探测时 404；真实模型在上游。 */
    @GetMapping("/v1/models")
    public ResponseEntity<byte[]> models() {
        String body = "{\"object\":\"list\",\"data\":[],\"note\":\"模型列表请查询上游 LLM\"}";
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(body.getBytes(StandardCharsets.UTF_8));
    }
}
