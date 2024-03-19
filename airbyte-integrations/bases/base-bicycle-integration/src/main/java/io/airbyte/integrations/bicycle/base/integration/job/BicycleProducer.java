package io.airbyte.integrations.bicycle.base.integration.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class BicycleProducer<T, R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BicycleProducer.class);

    private static AtomicLong counter = new AtomicLong(0);
    private static AtomicLong threadcounter = new AtomicLong(0);
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
                new ArrayBlockingQueue<>(poolSize), new ThreadFactory() {

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

    public void stop(boolean force) {
        int retries = 0;
        for (Future<R> future : futures.keySet()) {
            ProducerJob job = futures.get(future);
            job.finish();
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
        }
        for (Future<R> future : futures.keySet()) {
            if (!future.isDone()) {
                future.cancel(true);
            }
        }
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
                        counter.incrementAndGet();
                        if (counter.get() % 2000 == 0) {
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
                                    misses = 0;
                                }
                            } catch (InterruptedException e) {
                            }
                        } while (!added && misses < 5000);
                        if (!added) {
                            LOGGER.info("[{}] Skipped Records : [{}]", Thread.currentThread().getName(), counter.get());
                        }
                    }
                });
            } catch (Exception e) {
                LOGGER.error("BicycleProducer", e);
            }
        }
    }
}
