package io.airbyte.integrations.bicycle.base.integration.job.consumer;

import io.airbyte.integrations.bicycle.base.integration.job.config.ConsumerConfig;
import io.airbyte.integrations.bicycle.base.integration.job.metrics.EventProcessMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class BicycleConsumer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BicycleConsumer.class);

    private AtomicLong counter = new AtomicLong(0);
    private AtomicLong threadcounter = new AtomicLong(0);
    private String name;
    private String identifier;
    private ConsumerConfig config;
    private EventProcessMetrics metrics;
    private ExecutorService executorService;
    private BlockingQueue<T> queue;
    private Map<Future, ConsumerJob> futures = new HashMap<>();

    public BicycleConsumer(ConsumerConfig config, BlockingQueue<T> queue, EventProcessMetrics metrics) {
        this.name = config.getName();
        this.identifier = config.getIdentifier();
        this.config = config;
        this.metrics = metrics;
        this.queue = queue;
        executorService = new ThreadPoolExecutor(config.getPoolSize(), config.getPoolSize(), 600, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(config.getMaxExecutorPoolQueue()), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, name+"-"+identifier+"-"+ threadcounter.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    public void stop() {
        for (Future<?> future : futures.keySet()) {
            ConsumerJob job = futures.get(future);
            job.finish();
        }
        for (Future<?> future : futures.keySet()) {
            int retries = 0;
            boolean done;
            do {
                done = future.isDone();
                if (!done) {
                    try {
                        Thread.sleep(config.getSleepTimeInMillis());
                    } catch (InterruptedException e) {
                    }
                    retries++;
                }
            } while (retries < config.getMaxIdlePollsRetries() && !done);
            if (!future.isDone()) {
                future.cancel(true);
            }
        }
        LOGGER.info("[{}] Total events received on queue : [{}] queue size[{}]", Thread.currentThread().getName(),
                counter.get(), queue.size());
    }

    public void shutdown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    public Future submit(ConsumerJob<T> job) {
        Future<?> future = executorService.submit(new IConsumer(job, queue));
        futures.put(future, job);
        return future;
    }

    class IConsumer implements Runnable {

        private ConsumerJob<T> job;
        private BlockingQueue<T> queue;

        public IConsumer(ConsumerJob<T> job, BlockingQueue<T> queue) {
            this.job = job;
            this.queue = queue;
        }

        public void run() {
            try {
                int misses = 0;
                do {
                    T t = queue.poll(config.getSleepTimeInMillis(), TimeUnit.MILLISECONDS);
                    if (t != null) {
                        counter.incrementAndGet();
                        if (counter.get() % 1000 == 0) {
                            LOGGER.info("[{}] Receiving from queue : [{}] queue size[{}]", Thread.currentThread().getName(),
                                    counter.get(), queue.size());
                        }
                        misses = 0;
                        try {
                            job.process(t);
                        } catch (Exception e) {
                            LOGGER.error("[{}] Failed to consume data [{}]", Thread.currentThread().getName(), counter.get(), e);
                        }
                    } else {
                        LOGGER.info("[{}] Waiting to pull : [{}] size [{}]  misses [{}]",
                                Thread.currentThread().getName(), counter.get(), queue.size(), misses);
                        misses++;
                    }
                } while (misses < config.getMaxIdlePollsRetries());
            } catch (InterruptedException e) {
                LOGGER.error("BicycleConsumer [{}] [{}]", name, identifier, e);
            } finally {
                if (job != null) {
                    try {
                        job.finish();
                        LOGGER.info("Consumer Call Finished [{}] [{}]", name, identifier);
                    } catch (Exception e) {
                        LOGGER.error("Failed to finish consumer cleanly [{}] [{}]", name, identifier, e);
                    }
                }
            }
        }
    }

}
