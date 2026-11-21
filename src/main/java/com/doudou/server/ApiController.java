package com.doudou.server;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 服务的 HTTP 数据接口：健康检查、trace 上报/查询/详情、按模型汇总、实时推送。
 *
 * <p>不做认证、不做用户系统——这是本地/局域网里的个人可观测服务，不是对外产品。
 */
@RestController
@RequestMapping("/api")
public class ApiController {

    private final TraceStore store;
    private final TraceBroadcaster broadcaster;

    public ApiController(TraceStore store, TraceBroadcaster broadcaster) {
        this.store = store;
        this.broadcaster = broadcaster;
    }

    /** 实时推送：看板用 EventSource 订阅，新 trace 一到即推。 */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return broadcaster.subscribe();
    }

    /** 单条 trace 的正文详情（含请求体与原始回复），供看板展开审计。 */
    @GetMapping("/traces/{id}")
    public ResponseEntity<?> traceDetail(@PathVariable long id) {
        TraceStore.TraceDetail detail = store.findById(id);
        if (detail == null) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("error", "未找到 id=" + id + " 的 trace");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
        }
        return ResponseEntity.ok(detail);
    }

    /** 健康检查：App 设置页可以用它验证后端地址是否可达。 */
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", true);
        result.put("service", "doudou-server");
        return result;
    }

    /**
     * 接收一条 trace。端侧 TraceEvent.writeTo() 每次 POST 一条。
     */
    @PostMapping("/traces")
    public ResponseEntity<Map<String, Object>> ingest(@RequestBody TraceRecord record) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (record == null || record.getTs() <= 0 || record.getModel().isEmpty()) {
            result.put("saved", 0);
            result.put("reason", "trace 缺少 ts 或 model");
            return ResponseEntity.badRequest().body(result);
        }
        try {
            store.insert(record);
            result.put("saved", 1);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            result.put("saved", 0);
            result.put("reason", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    /** 按模型查询最近的 trace，用于排查一次具体调用。 */
    @GetMapping("/traces")
    public Map<String, Object> query(
            @RequestParam(required = false) String model,
            @RequestParam(defaultValue = "50") int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 500));
        List<TraceStore.TraceRow> rows = store.query(model, safeLimit);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("count", rows.size());
        result.put("traces", new ArrayList<>(rows));
        return result;
    }

    /** 按模型汇总，作为 Eval 的基线对比数据。 */
    @GetMapping("/stats")
    public Map<String, Object> stats() {
        List<TraceStore.SummaryRow> rows = store.summary();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groups", new ArrayList<>(rows));
        return result;
    }
}
