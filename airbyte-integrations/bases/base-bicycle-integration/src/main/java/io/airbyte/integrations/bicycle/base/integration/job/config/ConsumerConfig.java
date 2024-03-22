package io.airbyte.integrations.bicycle.base.integration.job.config;

public class ConsumerConfig {

    private String name;
    private String identifier;
    private int poolSize = 4;
    private int maxExecutorPoolQueue = 100;
    private int maxIdlePollsRetries = 1000;
    private int sleepTimeInMillis = 60;

    public static ConsumerConfig newBuilder() {
        return new ConsumerConfig();
    }

    public ConsumerConfig setName(String name) {
        this.name = name;
        return this;
    }

    public ConsumerConfig setIdentifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    public ConsumerConfig setPoolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    public ConsumerConfig setMaxIdlePollsRetries(int maxIdlePollsRetries) {
        this.maxIdlePollsRetries = maxIdlePollsRetries;
        return this;
    }

    public ConsumerConfig setSleepTimeInMillis(int sleepTimeInMillis) {
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

    public int getMaxIdlePollsRetries() {
        return maxIdlePollsRetries;
    }

    public int getSleepTimeInMillis() {
        return sleepTimeInMillis;
    }
}
