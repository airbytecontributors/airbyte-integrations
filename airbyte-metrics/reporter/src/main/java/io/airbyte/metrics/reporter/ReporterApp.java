/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter;

import io.airbyte.config.Configs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.db.Database;
import io.airbyte.db.instance.configs.ConfigsDatabaseInstance;
import io.airbyte.metrics.lib.DatadogClientConfiguration;
import io.airbyte.metrics.lib.DogStatsDMetricSingleton;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.airbyte.metrics.lib.MetricQueries;
import io.airbyte.metrics.lib.MetricsRegistry;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * This application collects and a publishes a set of general metrics useful to the operation of
 * Airbyte. In general, we avoid embedding metrics in applications as it often pollutes application
 * code.
 * <p>
 * This is primarily intended for Airbyte Cloud. However, any OSS user is free to turn these on for
 * consumption.
 * <p>
 * This currently only supports Datadog.
 * <p>
 * See
 * https://docs.google.com/document/d/11pEUsHyKUhh4CtV3aReau3SUG-ncEvy6ROJRVln6YB4/edit?usp=sharing
 * for more info.
 */
@Slf4j
public class ReporterApp {

  public static void main(final String[] args) throws IOException, InterruptedException {
    final Configs configs = new EnvConfigs();

    DogStatsDMetricSingleton.initialize(MetricEmittingApps.METRICS_REPORTER, new DatadogClientConfiguration(configs));

    final Database configDatabase = new ConfigsDatabaseInstance(
        configs.getConfigDatabaseUser(),
        configs.getConfigDatabasePassword(),
        configs.getConfigDatabaseUrl())
            .getInitialized();

    final var pollers = Executors.newScheduledThreadPool(4);

    log.info("Starting pollers..");
    pollers.scheduleAtFixedRate(() -> {
      try {
        final var pendingJobs = configDatabase.query(MetricQueries::numberOfPendingJobs);
        DogStatsDMetricSingleton.gauge(MetricsRegistry.NUM_PENDING_JOBS, pendingJobs);
      } catch (final SQLException e) {
        e.printStackTrace();
      }
    }, 0, 15, TimeUnit.SECONDS);
    pollers.scheduleAtFixedRate(() -> {
      try {
        final var runningJobs = configDatabase.query(MetricQueries::numberOfRunningJobs);
        DogStatsDMetricSingleton.gauge(MetricsRegistry.NUM_RUNNING_JOBS, runningJobs);
      } catch (final SQLException e) {
        e.printStackTrace();
      }
    }, 0, 15, TimeUnit.SECONDS);
    pollers.scheduleAtFixedRate(() -> {
      try {
        final var age = configDatabase.query(MetricQueries::oldestRunningJobAgeSecs);
        DogStatsDMetricSingleton.gauge(MetricsRegistry.OLDEST_RUNNING_JOB_AGE_SECS, age);
      } catch (final SQLException e) {
        e.printStackTrace();
      }
    }, 0, 15, TimeUnit.SECONDS);
    pollers.scheduleAtFixedRate(() -> {
      try {
        final var age = configDatabase.query(MetricQueries::oldestPendingJobAgeSecs);
        DogStatsDMetricSingleton.gauge(MetricsRegistry.OLDEST_PENDING_JOB_AGE_SECS, age);
      } catch (final SQLException e) {
        e.printStackTrace();
      }
    }, 0, 15, TimeUnit.SECONDS);

    Thread.sleep(1000_000 * 1000);
  }

}
