package com.doudou.server;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * 一条 trace，字段与端侧 TraceEvent.toJsonLine() 一一对应。
 *
 * <p>JsonIgnoreProperties 让端侧以后多塞字段也不会把整个请求打挂——
 * 可观测服务不能因为客户端升级就拒绝数据。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TraceRecord {

    private long ts;
    private String model;
    private String endpoint;
    private int round;
    private int sys;
    private int tool;
    private int mem;
    private int sum;
    private int hist;
    private int dropped;
    private int truncated;
    private boolean overflow;
    private List<String> calls;
    private String finish;
    private long ms;
    private boolean ok;
    private String err;
    /** 发给 LLM 的请求体原文（已剔除 x_doudou_ctx）；用于审计，存前会截断。 */
    private String requestBody;
    /** 上游 SSE 原始回复（逐行 data 帧拼接）；用于审计，存前会截断。 */
    private String responseRaw;

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getModel() {
        return model == null ? "" : model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getEndpoint() {
        return endpoint == null ? "" : endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public int getRound() {
        return round;
    }

    public void setRound(int round) {
        this.round = round;
    }

    public int getSys() {
        return sys;
    }

    public void setSys(int sys) {
        this.sys = sys;
    }

    public int getTool() {
        return tool;
    }

    public void setTool(int tool) {
        this.tool = tool;
    }

    public int getMem() {
        return mem;
    }

    public void setMem(int mem) {
        this.mem = mem;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public int getHist() {
        return hist;
    }

    public void setHist(int hist) {
        this.hist = hist;
    }

    public int getDropped() {
        return dropped;
    }

    public void setDropped(int dropped) {
        this.dropped = dropped;
    }

    public int getTruncated() {
        return truncated;
    }

    public void setTruncated(int truncated) {
        this.truncated = truncated;
    }

    public boolean isOverflow() {
        return overflow;
    }

    public void setOverflow(boolean overflow) {
        this.overflow = overflow;
    }

    public List<String> getCalls() {
        return calls;
    }

    public void setCalls(List<String> calls) {
        this.calls = calls;
    }

    public String getFinish() {
        return finish == null ? "" : finish;
    }

    public void setFinish(String finish) {
        this.finish = finish;
    }

    public long getMs() {
        return ms;
    }

    public void setMs(long ms) {
        this.ms = ms;
    }

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    public String getErr() {
        return err == null ? "" : err;
    }

    public void setErr(String err) {
        this.err = err;
    }

    public String getRequestBody() {
        return requestBody == null ? "" : requestBody;
    }

    public void setRequestBody(String requestBody) {
        this.requestBody = requestBody;
    }

    public String getResponseRaw() {
        return responseRaw == null ? "" : responseRaw;
    }

    public void setResponseRaw(String responseRaw) {
        this.responseRaw = responseRaw;
    }

    /** 上下文各层 token 之和，与端侧 totalContextTokens() 同口径。 */
    public int totalContextTokens() {
        return sys + tool + mem + sum + hist;
    }

    public String callsText() {
        if (calls == null || calls.isEmpty()) {
            return "";
        }
        return String.join(",", calls);
    }
}
