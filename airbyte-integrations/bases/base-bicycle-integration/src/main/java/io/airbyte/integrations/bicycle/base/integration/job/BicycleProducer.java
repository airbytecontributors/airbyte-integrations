package io.airbyte.integrations.bicycle.base.integration.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class BicycleProducer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BicycleProducer.class);

    private AtomicLong counter = new AtomicLong(0);
    private AtomicLong threadcounter = new AtomicLong(0);
    private volatile boolean stop = false;
    private String name;
    private EventProcessMetrics metrics;
    private BlockingQueue<T> queue;
    private ExecutorService executorService;
    private Map<Future, ProducerJob> futures = new HashMap<>();

    public BicycleProducer(String name, int poolSize, BlockingQueue<T> queue, EventProcessMetrics metrics) {
        this.name = name;
        this.queue = queue;
        this.metrics = metrics;
        executorService = new ThreadPoolExecutor(poolSize, poolSize, 600, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1000),
                new ThreadFactory() {
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r, name+"-"+ threadcounter.incrementAndGet());
                        thread.setDaemon(true);
                        return thread;
                    }
        });
    }

    public Future submit(ProducerJob<T> job) {
        Future<?> future = executorService.submit(new IProducer(job, queue));
        futures.put(future, job);
        return future;
    }

    public void stop() {
        for (Future<?> future : futures.keySet()) {
            ProducerJob job = futures.get(future);
            job.finish();
        }
        stop = true;
        for (Future<?> future : futures.keySet()) {
            int retries = 0;
            boolean done = false;
            do {
                done = future.isDone();
                if (!done) {
                    try {
                        Thread.sleep(120);
                    } catch (InterruptedException e) {
                    }
                    retries++;
                }
            } while (retries < 1000 && !done);
            if (!future.isDone()) {
                future.cancel(true);
            }
        }
    }

    public boolean addToQueue(T o) {
        counter.incrementAndGet();
        if (counter.get() % 1000 == 0) {
            LOGGER.info("[{}] Adding to queue : [{}]", Thread.currentThread().getName(), counter.get());
        }
        int misses = 0;
        boolean added = false;
        do {
            added = queue.offer(o);
            try {
                if (!added) {
                    LOGGER.info("[{}] Waiting to push : [{}] size [{}] misses [{}]",
                            Thread.currentThread().getName(), counter.get(), queue.size(), misses);
                    Thread.sleep(60);
                    misses++;
                } else {
                    return true;
                }
            } catch (InterruptedException e) {
            }
        } while (!added && misses < 5000);
        if (!added) {
            LOGGER.info("[{}] Skipped Records : [{}]", Thread.currentThread().getName(), counter.get());
        }
        return false;
    }


    class IProducer implements Runnable {

        private final ProducerJob<T> job;
        private final BlockingQueue<T> queue;

        public IProducer(ProducerJob<T> job, BlockingQueue<T> queue) {
            this.job = job;
            this.queue = queue;
        }

        public void run() {
            try {
                job.process(new Producer<T>() {
                    public void produce(T o) {
                        addToQueue(o);
                    }
                });
            } catch (Exception e) {
                LOGGER.error("BicycleProducer", e);
            }
        }
    }
}
