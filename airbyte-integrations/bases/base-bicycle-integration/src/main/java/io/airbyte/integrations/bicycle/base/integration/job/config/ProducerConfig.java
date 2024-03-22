package io.airbyte.integrations.bicycle.base.integration.job.config;

public class ProducerConfig {


    private String name;
    private String identifier;
    private int poolSize = 4;
    private int maxBlockingQueueSize = 100;
    private int maxExecutorPoolQueue = 100;
    private int maxRetries = 1000;
    private int sleepTimeInMillis = 60;

    public static ProducerConfig newBuilder() {
        return new ProducerConfig();
    }

    public ProducerConfig setName(String name) {
        this.name = name;
        return this;
    }

    public ProducerConfig setIdentifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    public ProducerConfig setPoolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    public ProducerConfig setMaxBlockingQueueSize(int maxBlockingQueueSize) {
        this.maxBlockingQueueSize = maxBlockingQueueSize;
        return this;
    }

    public ProducerConfig setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public ProducerConfig setSleepTimeInMillis(int sleepTimeInMillis) {
        this.sleepTimeInMillis = sleepTimeInMillis;
        return this;
    }

    public String getName() {
        return name;
    }
    public String getIdentifier() {
        return identifier;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public int getMaxExecutorPoolQueue() {
        return maxExecutorPoolQueue;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getMaxBlockingQueueSize() {
        return maxBlockingQueueSize;
    }

    public int getSleepTimeInMillis() {
        return sleepTimeInMillis;
    }
}
