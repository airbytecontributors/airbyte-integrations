package io.airbyte.scheduler;

import io.airbyte.config.JobGetSpecConfig;
import io.airbyte.config.JobOutput;
import io.airbyte.workers.DefaultGetSpecWorker;
import io.airbyte.workers.GetSpecWorker;
import io.airbyte.workers.OutputAndStatus;
import io.airbyte.workers.Worker;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.process.ProcessBuilderFactory;
import io.airbyte.workers.wrappers.JobOutputGetSpecWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

public class GetSpecWorkerRun {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetSpecWorkerRun.class);

    private final ProcessBuilderFactory pbf;
    private final WorkerRunFactory.Creator jobCreator;
    private final Path jobRoot;
    public GetSpecWorkerRun(final ProcessBuilderFactory pbf, final WorkerRunFactory.Creator jobCreator, final Path jobRoot) {
        this.jobCreator = jobCreator;
        this.jobRoot = jobRoot;
        this.pbf = pbf;
    }

    public WorkerRun create(long jobId, int attempt, JobGetSpecConfig config) throws Exception {
        final IntegrationLauncher launcher = new AirbyteIntegrationLauncher(jobId, attempt, config.getDockerImage(), pbf);

        WorkerRun workerRun = jobCreator.create(
                jobRoot,
                config,
                new JobOutputGetSpecWorker(new DefaultGetSpecWorker(launcher)));

        LOGGER.info("Executing worker wrapper...");
        Files.createDirectories(jobRoot);

        return workerRun;
    }

    // almost all it takes to turn this into a temporal activity
    public OutputAndStatus<JobOutput> run(long jobId, int attempt, JobGetSpecConfig config) {
        try {
            return create(jobId, attempt, config).call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
