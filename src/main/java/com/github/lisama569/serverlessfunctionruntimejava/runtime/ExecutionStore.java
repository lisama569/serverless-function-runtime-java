package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Component;

import com.github.lisama569.serverlessfunctionruntimejava.model.ExecutionRecord;

/**
 * 进程内执行记录持久化存储。
 * 以 executionId 为主索引、idempotencyKey 为唯一索引，
 * 保证相同幂等键的重复提交命中同一条记录，不产生重复副作用。
 */
@Component
public class ExecutionStore {

    private final ConcurrentMap<String, ExecutionRecord> byId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ExecutionRecord> byIdempotencyKey = new ConcurrentHashMap<>();

    public ExecutionRecord save(ExecutionRecord record) {
        byId.put(record.getExecutionId(), record);
        return record;
    }

    public Optional<ExecutionRecord> findById(String executionId) {
        return Optional.ofNullable(byId.get(executionId));
    }

    /**
     * 幂等键原子去重：若该键已有记录则返回已有记录（调用方应直接复用），
     * 否则放入新记录并返回 Optional.empty()。
     */
    public Optional<ExecutionRecord> putIfAbsentIdempotencyKey(String idempotencyKey, ExecutionRecord record) {
        return Optional.ofNullable(byIdempotencyKey.putIfAbsent(idempotencyKey, record));
    }

    /** 仅当映射仍指向 expected 时移除（用于拒绝后释放幂等键）。 */
    public void removeIdempotencyMapping(String idempotencyKey, ExecutionRecord expected) {
        byIdempotencyKey.remove(idempotencyKey, expected);
    }

    public Optional<ExecutionRecord> findByIdempotencyKey(String idempotencyKey) {
        return Optional.ofNullable(byIdempotencyKey.get(idempotencyKey));
    }
}
