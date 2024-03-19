package io.airbyte.integrations.bicycle.base.integration.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class BicycleConsumer<T, R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BicycleConsumer.class);

    private static AtomicLong counter = new AtomicLong(0);
    private static AtomicLong threadcounter = new AtomicLong(0);
    private String name;
    private EventProcessMetrics metrics;
    private ExecutorService executorService;
    private Map<Future, ConsumerJob> futures = new HashMap<>();

    public BicycleConsumer(String name, int poolSize, BlockingQueue<T> queue, ConsumerJob<T> job,
                           EventProcessMetrics metrics) {
        this.name = name;
        this.metrics = metrics;
        executorService = new ThreadPoolExecutor(poolSize, poolSize, 600, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(poolSize), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, name+"-"+ threadcounter.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }
        });
        for (int i =0; i < poolSize; i++) {
            Future<R> future = (Future<R>) executorService.submit(new IConsumer(job, queue));
            futures.put(future, job);
        }
    }


    public void stop(boolean force) {
        int retries = 0;
        for (Future<R> future : futures.keySet()) {
            ConsumerJob job = futures.get(future);
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
                    T t = queue.poll(10, TimeUnit.SECONDS);
                    if (t != null) {
                        counter.incrementAndGet();
                        if (counter.get() % 2000 == 0) {
                            LOGGER.info("[{}] Receiving from queue : [{}]", Thread.currentThread().getName(),
                                                                            counter.get());
                        }
                        misses = 0;
                        job.process(t);
                    } else {
                        LOGGER.info("[{}] Waiting to pull : [{}] size [{}]  misses [{}]",
                                Thread.currentThread().getName(), counter.get(), queue.size(), misses);
                        misses++;
                    }
                } while (misses < 10);
            } catch (InterruptedException e) {
                LOGGER.error("BicycleConsumer", e);
            }
        }
    }

}
