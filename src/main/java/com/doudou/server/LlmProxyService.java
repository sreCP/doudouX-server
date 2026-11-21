package com.doudou.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * OpenAI 兼容的流式代理：App → 网关 → 上游 LLM → 网关 → App。
 *
 * <p>按行透传 SSE，不缓冲整个响应，保证 App 的打字机效果；
 * 透传的同时从帧里抽出 model / 工具名 / finish_reason / 首字延迟 / 耗时，
 * 在流结束后自动记一条 trace——trace 由网关产生，端侧不再上报。
 *
 * <p>App 可以在请求体顶层带一个 {@code x_doudou_ctx} 对象，把端侧上下文预算的
 * 分层 token 告诉网关；网关解析后一并写进 trace，弥补"代理层看不到端侧上下文"的缺口。
 * 该字段只在网关消费，转发给上游前会剔除，避免污染 LLM 请求。
 */
@Service
public class LlmProxyService {

    private static final Logger log = LoggerFactory.getLogger(LlmProxyService.class);

    /**
     * 审计正文上限。用户要求保留完整 raw 回复，所以放到 64MB 这个"只防异常无限流"的量级：
     * 正常几 MB 的回复不会触发，只有上游异常狂喷时才会兜底截断，避免 OOM。
     */
    private static final int MAX_REQUEST_CHARS = 64 * 1024 * 1024;
    private static final int MAX_RESPONSE_CHARS = 64 * 1024 * 1024;

    private final ProxyProperties props;
    private final TraceStore store;
    private final TraceBroadcaster broadcaster;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http;

    public LlmProxyService(ProxyProperties props, TraceStore store, TraceBroadcaster broadcaster) {
        this.props = props;
        this.store = store;
        this.broadcaster = broadcaster;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(props.getConnectTimeoutSeconds()))
                .build();
    }

    /** 返回一个流式响应体：调用方（Controller）直接交给 Spring 即可。 */
    public StreamingResponseBody proxy(byte[] rawBody) {
        final String upstream = props.chatCompletionsUrl();
        final byte[] forwardBody = stripContextField(rawBody);
        final String model = parseModel(rawBody);
        final ContextInfo ctx = parseContext(rawBody);

        return output -> {
            long start = System.currentTimeMillis();
            long firstTokenMs = -1;
            String finish = "";
            Set<String> toolNames = new LinkedHashSet<>();
            boolean ok = true;
            boolean clientDisconnected = false;
            String err = "";
            // 审计用：完整拼接上游原始 SSE 行，最后按上限截断后入库
            final StringBuilder rawBuilder = new StringBuilder();

            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(upstream))
                        .timeout(Duration.ofMinutes(10))
                        .header("Content-Type", "application/json; charset=utf-8")
                        .header("Accept", "text/event-stream")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(forwardBody))
                        .build();

                HttpResponse<InputStream> response =
                        http.send(request, HttpResponse.BodyHandlers.ofInputStream());

                int status = response.statusCode();
                if (status < 200 || status >= 300) {
                    ok = false;
                    err = "上游返回 HTTP " + status;
                    byte[] errorBody = response.body().readAllBytes();
                    output.write(errorBody);
                    output.flush();
                    log.warn("代理：上游 HTTP {}，错误体已透传（{} 字节）", status, errorBody.length);
                } else {
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(response.body(), StandardCharsets.UTF_8));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        // 原样逐行回写，保证流式；write 后立即 flush
                        output.write(line.getBytes(StandardCharsets.UTF_8));
                        output.write('\n');
                        output.flush();

                        // 审计留存：只留前 MAX_RESPONSE_CHARS 个字符，超出不再追加
                        if (rawBuilder.length() < MAX_RESPONSE_CHARS) {
                            rawBuilder.append(line).append('\n');
                        }

                        if (!line.startsWith("data:")) {
                            continue;
                        }
                        String payload = line.substring(5).trim();
                        if (payload.isEmpty() || "[DONE]".equals(payload)) {
                            continue;
                        }
                        EventInfo info = parseEvent(payload);
                        if (info.finish != null && !info.finish.isEmpty()) {
                            finish = info.finish;
                        }
                        if (info.tools != null) {
                            toolNames.addAll(info.tools);
                        }
                        if (firstTokenMs < 0 && info.hasContent) {
                            firstTokenMs = System.currentTimeMillis() - start;
                        }
                    }
                }
            } catch (Exception e) {
                err = e.getMessage() == null ? e.toString() : e.getMessage();
                // 客户端提前断开（用户划走、网络切换）不是模型失败：
                // 上半段流已经正常回给 App，只记 cancel，不污染失败率。
                clientDisconnected = err.contains("ServletOutputStream")
                        || err.contains("中止了一个已建立的连接")
                        || err.contains("Broken pipe")
                        || err.contains("Connection reset");
                if (clientDisconnected) {
                    finish = "cancel";
                    log.info("客户端提前断开，流已转发部分内容，记为 cancel");
                } else {
                    ok = false;
                    log.warn("代理失败：{}", err);
                }
            } finally {
                long elapsed = System.currentTimeMillis() - start;
                try {
                    TraceRecord trace = new TraceRecord();
                    trace.setTs(System.currentTimeMillis());
                    trace.setModel(model.isEmpty() ? "unknown" : model);
                    trace.setEndpoint(upstream);
                    trace.setRound(ctx.round > 0 ? ctx.round : 1);
                    trace.setSys(ctx.sys);
                    trace.setTool(ctx.tool);
                    trace.setMem(ctx.mem);
                    trace.setSum(ctx.sum);
                    trace.setHist(ctx.hist);
                    trace.setDropped(ctx.dropped);
                    trace.setTruncated(ctx.truncated);
                    trace.setOverflow(ctx.overflow);
                    trace.setCalls(new ArrayList<>(toolNames));
                    trace.setFinish(finish.isEmpty() ? (ok ? "stop" : "error") : finish);
                    trace.setMs(elapsed);
                    trace.setOk(ok);
                    trace.setErr(clientDisconnected ? "" : err);
                    // 审计内容：真正发给 LLM 的请求体（已剔除 x_doudou_ctx）+ 原始 SSE 回复
                    trace.setRequestBody(clip(new String(forwardBody, StandardCharsets.UTF_8),
                            MAX_REQUEST_CHARS));
                    trace.setResponseRaw(clip(rawBuilder.toString(), MAX_RESPONSE_CHARS));
                    long id = store.insert(trace);
                    broadcaster.publish(toEvent(id, trace));
                    log.info("代理完成：model={}，工具={}，耗时={}ms，首字={}ms，成功={}，finish={}",
                            trace.getModel(), toolNames, elapsed, firstTokenMs, ok, trace.getFinish());
                } catch (Exception e) {
                    // trace 是旁路，写失败绝不影响已经回给 App 的流
                    log.warn("代理 trace 记录失败（不影响响应）：{}", e.getMessage());
                }
            }
        };
    }

    /** 透传前剔除端侧的 x_doudou_ctx，避免把它当业务字段发给 LLM。 */
    private byte[] stripContextField(byte[] raw) {
        try {
            JsonNode root = mapper.readTree(raw);
            if (root instanceof com.fasterxml.jackson.databind.node.ObjectNode) {
                ((com.fasterxml.jackson.databind.node.ObjectNode) root).remove("x_doudou_ctx");
                return mapper.writeValueAsBytes(root);
            }
        } catch (Exception ignored) {
            // 请求体不是合法 JSON 时原样转发，让上游去报错
        }
        return raw;
    }

    private String parseModel(byte[] raw) {
        try {
            JsonNode node = mapper.readTree(raw).get("model");
            return node == null ? "" : node.asText();
        } catch (Exception e) {
            return "";
        }
    }

    /** 读取 App 附带的上下文分层统计；缺字段就返回全 0，不影响代理。 */
    private ContextInfo parseContext(byte[] raw) {
        ContextInfo info = new ContextInfo();
        try {
            JsonNode node = mapper.readTree(raw).get("x_doudou_ctx");
            if (node == null || node.isNull()) {
                return info;
            }
            info.round = node.path("round").asInt(0);
            info.sys = node.path("sys").asInt(0);
            info.tool = node.path("tool").asInt(0);
            info.mem = node.path("mem").asInt(0);
            info.sum = node.path("sum").asInt(0);
            info.hist = node.path("hist").asInt(0);
            info.dropped = node.path("dropped").asInt(0);
            info.truncated = node.path("truncated").asInt(0);
            info.overflow = node.path("overflow").asBoolean(false);
        } catch (Exception ignored) {
            // 上下文统计是增强信息，解析失败用默认值
        }
        return info;
    }

    /** 从一帧 SSE 里抽 finish_reason / 工具名 / 是否有正文增量。 */
    private EventInfo parseEvent(String payload) {
        EventInfo info = new EventInfo();
        try {
            JsonNode root = mapper.readTree(payload);
            JsonNode choices = root.get("choices");
            if (choices == null || !choices.isArray() || choices.isEmpty()) {
                return info;
            }
            JsonNode first = choices.get(0);

            JsonNode finish = first.get("finish_reason");
            if (finish != null && !finish.isNull()) {
                info.finish = finish.asText();
            }

            JsonNode delta = first.get("delta");
            if (delta == null || delta.isNull()) {
                return info;
            }
            JsonNode content = delta.get("content");
            JsonNode reasoning = delta.get("reasoning_content");
            JsonNode reasoningAlt = delta.get("reasoning"); // Ollama 用这个字段名
            info.hasContent = hasText(content) || hasText(reasoning) || hasText(reasoningAlt);

            JsonNode calls = delta.get("tool_calls");
            if (calls != null && calls.isArray()) {
                for (JsonNode call : calls) {
                    JsonNode function = call.get("function");
                    if (function == null) {
                        continue;
                    }
                    JsonNode name = function.get("name");
                    if (name != null && !name.isNull() && !name.asText().isEmpty()) {
                        info.tools.add(name.asText());
                    }
                }
            }
        } catch (Exception ignored) {
            // 单帧解析失败不影响透传
        }
        return info;
    }

    private static boolean hasText(JsonNode node) {
        return node != null && !node.isNull() && !node.asText().isEmpty();
    }

    /** 构造推送给看板的精简事件：字段名与 /api/traces 列表一致，正文只给长度。 */
    private Map<String, Object> toEvent(long id, TraceRecord t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("ts", t.getTs());
        m.put("model", t.getModel());
        m.put("round", t.getRound());
        m.put("sys", t.getSys());
        m.put("tool", t.getTool());
        m.put("mem", t.getMem());
        m.put("sum", t.getSum());
        m.put("hist", t.getHist());
        m.put("dropped", t.getDropped());
        m.put("truncated", t.getTruncated());
        m.put("overflow", t.isOverflow());
        m.put("calls", t.callsText());
        m.put("finish", t.getFinish());
        m.put("ms", t.getMs());
        m.put("ok", t.isOk());
        m.put("err", t.getErr());
        m.put("requestBodyLength", t.getRequestBody().length());
        m.put("responseRawLength", t.getResponseRaw().length());
        return m;
    }

    /** 按字符上限截断，超出加省略标记，保证审计内容有边界。 */
    private static String clip(String text, int max) {
        if (text == null) {
            return "";
        }
        if (text.length() <= max) {
            return text;
        }
        return text.substring(0, max) + "\n…（已截断，原始长度 " + text.length() + " 字符）";
    }

    private static class EventInfo {
        String finish = "";
        boolean hasContent = false;
        final Set<String> tools = new LinkedHashSet<>();
    }

    private static class ContextInfo {
        int round;
        int sys;
        int tool;
        int mem;
        int sum;
        int hist;
        int dropped;
        int truncated;
        boolean overflow;
    }
}
