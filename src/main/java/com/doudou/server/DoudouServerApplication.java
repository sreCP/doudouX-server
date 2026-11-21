package com.doudou.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 兜兜X 服务端。
 *
 * <p>两个身份合在一个进程里：
 * <ul>
 *   <li><b>LLM 流式代理网关</b>：App 把请求打到 {@code /v1/chat/completions}，
 *       这里转发给上游模型并逐行透传，链路中顺带记下 trace；</li>
 *   <li><b>可观测后端</b>：trace 落 SQLite，通过 {@code /api/*} 提供查询、
 *       汇总与 SSE 实时推送，根路径 {@code /} 是自带的看板页面。</li>
 * </ul>
 */
@SpringBootApplication
public class DoudouServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DoudouServerApplication.class, args);
    }
}
