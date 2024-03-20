package io.airbyte.integrations.bicycle.base.integration.job;

import java.util.concurrent.atomic.AtomicLong;

public class EventProcessMetrics {

    private AtomicLong success = new AtomicLong(0);
    private AtomicLong failed = new AtomicLong(0);
    private long totalRecords;

    public EventProcessMetrics(long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public long getSuccess() {
        return success.get();
    }

    public long success(int delta) {
        return success.addAndGet(delta);
    }

    public long getFailed() {
        return failed.get();
    }

    public long failed(int delta) {
        return failed.addAndGet(delta);
    }

    public long getTotalRecords() {
        return totalRecords;
    }
}
