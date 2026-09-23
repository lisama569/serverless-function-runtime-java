package com.github.lisama569.serverlessfunctionruntimejava.runtime;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 进程内持久化执行存储：按执行 ID 与幂等键索引。 */
public class ExecutionStore {

    private final ConcurrentHashMap<String, ExecutionRecord> byId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> idByIdempotencyKey = new ConcurrentHashMap<>();

    /** 若幂等键已存在则返回已有记录，否则原子地写入新记录。 */
    public ExecutionRecord putIfAbsent(ExecutionRecord record) {
        String existing = idByIdempotencyKey.putIfAbsent(record.idempotencyKey(), record.executionId());
        if (existing != null) {
            return byId.get(existing);
        }
        byId.put(record.executionId(), record);
        return record;
    }

    public void update(ExecutionRecord record) {
        byId.put(record.executionId(), record);
    }

    public Optional<ExecutionRecord> findById(String executionId) {
        return Optional.ofNullable(byId.get(executionId));
    }

    public Optional<ExecutionRecord> findByIdempotencyKey(String idempotencyKey) {
        String id = idByIdempotencyKey.get(idempotencyKey);
        return id == null ? Optional.empty() : Optional.ofNullable(byId.get(id));
    }

    public List<ExecutionRecord> findAll() {
        return List.copyOf(byId.values());
    }
}
