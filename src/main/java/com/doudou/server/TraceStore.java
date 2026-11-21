package com.doudou.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

/** trace 的 SQLite 存储层：只用 JdbcTemplate，不引入 ORM。 */
@Repository
public class TraceStore {

    private static final Logger log = LoggerFactory.getLogger(TraceStore.class);

    private final JdbcTemplate jdbc;
    private final int maxTraces;
    private final long maxTotalBytes;

    public TraceStore(JdbcTemplate jdbc,
                      @Value("${doudou.max-traces:5000}") int maxTraces,
                      @Value("${doudou.max-total-mb:512}") int maxTotalMb,
                      @Value("${spring.datasource.url}") String dbUrl) {
        this.jdbc = jdbc;
        this.maxTraces = maxTraces;
        this.maxTotalBytes = Math.max(0L, (long) maxTotalMb * 1024 * 1024);
        ensureDbDirectory(dbUrl);
        init();
    }

    /** SQLite 不会自动创建父目录，这里先建好，保证项目换台机器也能启动。 */
    private static void ensureDbDirectory(String dbUrl) {
        String prefix = "jdbc:sqlite:";
        if (dbUrl == null || !dbUrl.startsWith(prefix)) {
            return;
        }
        String path = dbUrl.substring(prefix.length());
        File file = new File(path);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("无法创建数据库目录：" + parent.getAbsolutePath());
        }
    }

    private void init() {
        jdbc.execute("CREATE TABLE IF NOT EXISTS traces ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                + "ts INTEGER NOT NULL,"
                + "model TEXT NOT NULL,"
                + "endpoint TEXT,"
                + "round INTEGER,"
                + "sys INTEGER, tool INTEGER, mem INTEGER, sum_tokens INTEGER, hist INTEGER,"
                + "dropped INTEGER, truncated INTEGER, overflow INTEGER,"
                + "calls TEXT, finish TEXT, ms INTEGER, ok INTEGER, err TEXT,"
                + "request_body TEXT, response_raw TEXT"
                + ")");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_traces_ts ON traces(ts DESC)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_traces_model ON traces(model)");
        // 已有库升级：CREATE TABLE IF NOT EXISTS 不会补列，这里显式检查并 ALTER
        ensureColumn("request_body", "TEXT");
        ensureColumn("response_raw", "TEXT");
    }

    private void ensureColumn(String name, String type) {
        Boolean exists = jdbc.queryForObject(
                "SELECT COUNT(*) > 0 FROM pragma_table_info('traces') WHERE name = ?",
                Boolean.class, name);
        if (exists != null && !exists) {
            jdbc.execute("ALTER TABLE traces ADD COLUMN " + name + " " + type);
        }
    }

    /** 插入一条 trace，返回自增 id（实时推送与详情查询都要用）。 */
    public long insert(TraceRecord t) {
        final String sql = "INSERT INTO traces (ts, model, endpoint, round, sys, tool, mem, "
                + "sum_tokens, hist, dropped, truncated, overflow, calls, finish, ms, ok, err, "
                + "request_body, response_raw) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        final Object[] params = new Object[]{
                t.getTs(), t.getModel(), t.getEndpoint(), t.getRound(),
                t.getSys(), t.getTool(), t.getMem(), t.getSum(), t.getHist(),
                t.getDropped(), t.getTruncated(), t.isOverflow() ? 1 : 0,
                t.callsText(), t.getFinish(), t.getMs(), t.isOk() ? 1 : 0, t.getErr(),
                t.getRequestBody(), t.getResponseRaw()};

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbc.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();

        // 保留策略一：按条数上限删最旧，避免无限增长
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM traces", Long.class);
        if (count != null && count > maxTraces) {
            jdbc.update("DELETE FROM traces WHERE id IN ("
                    + "SELECT id FROM traces ORDER BY id ASC LIMIT ?)", count - maxTraces);
        }

        // 保留策略二：按正文总字节数上限删最旧。存正文后单条可达数百 KB，
        // 只靠条数控制不住体积，这里再兜一层。
        if (maxTotalBytes > 0) {
            Long bytes = jdbc.queryForObject(
                    "SELECT COALESCE(SUM(LENGTH(request_body) + LENGTH(response_raw)), 0) FROM traces",
                    Long.class);
            int guard = 0;
            while (bytes != null && bytes > maxTotalBytes && guard++ < 50) {
                int deleted = jdbc.update(
                        "DELETE FROM traces WHERE id IN (SELECT id FROM traces ORDER BY id ASC LIMIT 100)");
                if (deleted == 0) {
                    break;
                }
                bytes = jdbc.queryForObject(
                        "SELECT COALESCE(SUM(LENGTH(request_body) + LENGTH(response_raw)), 0) FROM traces",
                        Long.class);
            }
            if (guard > 0) {
                log.info("按体积淘汰最旧 trace，剩余正文约 {} KB", bytes == null ? 0 : bytes / 1024);
            }
        }
        return key == null ? -1 : key.longValue();
    }

    public List<TraceRow> query(String model, int limit) {
        String sql;
        Object[] args;
        // 列表不查正文（可能很大），正文走 findById 单独取
        if (model == null || model.isEmpty()) {
            sql = "SELECT id, ts, model, round, sys, tool, mem, sum_tokens, hist, "
                    + "dropped, truncated, overflow, calls, finish, ms, ok, err, "
                    + "LENGTH(request_body) AS req_len, LENGTH(response_raw) AS res_len "
                    + "FROM traces ORDER BY ts DESC LIMIT ?";
            args = new Object[]{limit};
        } else {
            sql = "SELECT id, ts, model, round, sys, tool, mem, sum_tokens, hist, "
                    + "dropped, truncated, overflow, calls, finish, ms, ok, err, "
                    + "LENGTH(request_body) AS req_len, LENGTH(response_raw) AS res_len "
                    + "FROM traces WHERE model = ? ORDER BY ts DESC LIMIT ?";
            args = new Object[]{model, limit};
        }
        return jdbc.query(sql, (rs, i) -> new TraceRow(
                rs.getLong("id"),
                rs.getLong("ts"),
                rs.getString("model"),
                rs.getInt("round"),
                rs.getInt("sys"),
                rs.getInt("tool"),
                rs.getInt("mem"),
                rs.getInt("sum_tokens"),
                rs.getInt("hist"),
                rs.getInt("dropped"),
                rs.getInt("truncated"),
                rs.getInt("overflow") != 0,
                rs.getString("calls"),
                rs.getString("finish"),
                rs.getLong("ms"),
                rs.getInt("ok") != 0,
                rs.getString("err"),
                rs.getInt("req_len"),
                rs.getInt("res_len")), args);
    }

    /** 按 id 取单条 trace 的完整正文与 raw 回复，供看板详情面板使用。 */
    public TraceDetail findById(long id) {
        List<TraceDetail> rows = jdbc.query(
                "SELECT id, ts, model, finish, ms, ok, err, request_body, response_raw "
                        + "FROM traces WHERE id = ?",
                (rs, i) -> new TraceDetail(
                        rs.getLong("id"),
                        rs.getLong("ts"),
                        rs.getString("model"),
                        rs.getString("finish"),
                        rs.getLong("ms"),
                        rs.getInt("ok") != 0,
                        rs.getString("err"),
                        rs.getString("request_body"),
                        rs.getString("response_raw")), id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * 按模型分组汇总，这是 Eval 的基线数据来源：
     * 每次改预算参数或召回阈值后，对比各组的失败率与平均上下文 token。
     */
    public List<SummaryRow> summary() {
        String sql = "SELECT model, COUNT(*) AS n, "
                + "SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS fails, "
                + "AVG(sys + tool + mem + sum_tokens + hist) AS avg_tokens, "
                + "AVG(ms) AS avg_ms, SUM(dropped) AS dropped "
                + "FROM traces GROUP BY model ORDER BY n DESC";
        return jdbc.query(sql, (rs, i) -> new SummaryRow(
                rs.getString("model"),
                rs.getLong("n"),
                rs.getLong("fails"),
                rs.getDouble("avg_tokens"),
                rs.getDouble("avg_ms"),
                rs.getLong("dropped")));
    }

    /** 查询结果行（不含正文，只带长度，提示是否有详情可看）。 */
    public record TraceRow(long id, long ts, String model, int round, int sys, int tool, int mem,
                           int sum, int hist, int dropped, int truncated, boolean overflow,
                           String calls, String finish, long ms, boolean ok, String err,
                           int requestBodyLength, int responseRawLength) {
    }

    /** 单条 trace 的正文详情。 */
    public record TraceDetail(long id, long ts, String model, String finish, long ms,
                              boolean ok, String err, String requestBody, String responseRaw) {
    }

    /** 汇总行。 */
    public record SummaryRow(String model, long count, long failures,
                             double avgContextTokens, double avgLatencyMs, long dropped) {
    }
}
