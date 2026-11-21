package com.doudou.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 把新产生的 trace 实时推给看板。
 *
 * <p>用 SseEmitter 而不是 WebSocket：看板只需要单向推送，SSE 更轻、浏览器原生 EventSource 支持。
 * 连接超时设为 0（永不超时），由前端断线重连负责恢复。
 */
@Component
public class TraceBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(TraceBroadcaster.class);

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final ObjectMapper mapper = new ObjectMapper();

    /** 新的看板连接：加入列表，断开时自动移除。 */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        try {
            emitter.send(SseEmitter.event().name("ready").data("connected"));
        } catch (Exception e) {
            emitters.remove(emitter);
            log.warn("看板连接初始化失败：{}", e.getMessage());
        }
        log.info("看板已连接，当前在线 {}", emitters.size());
        return emitter;
    }

    /** 广播一条新 trace（不含正文，正文按需查详情接口，避免推送过大）。 */
    public void publish(Object payload) {
        if (emitters.isEmpty()) {
            return;
        }
        String json;
        try {
            json = mapper.writeValueAsString(payload);
        } catch (Exception e) {
            log.warn("trace 推送序列化失败：{}", e.getMessage());
            return;
        }
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("trace").data(json));
            } catch (Exception e) {
                // 客户端已断开：移除，不影响其他订阅者
                emitters.remove(emitter);
            }
        }
    }
}
