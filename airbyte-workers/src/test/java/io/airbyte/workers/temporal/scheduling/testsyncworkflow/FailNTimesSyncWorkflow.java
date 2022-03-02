/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testsyncworkflow;

import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.scheduler.models.IntegrationLauncherConfig;
import io.airbyte.scheduler.models.JobRunConfig;
import io.airbyte.workers.temporal.TemporalUtils;
import io.airbyte.workers.temporal.sync.SyncWorkflow;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.util.UUID;

public class FailNTimesSyncWorkflow implements SyncWorkflow {
    // Should match activity types from FailureHelper.java

    private final FailNTimesReplicationActivity activity = Workflow.newActivityStub(FailNTimesReplicationActivity.class, ActivityOptions.newBuilder()
            .setRetryOptions(TemporalUtils.NO_RETRY)
            .build());

    @Override
    public StandardSyncOutput run(final JobRunConfig jobRunConfig,
                                  final IntegrationLauncherConfig sourceLauncherConfig,
                                  final IntegrationLauncherConfig destinationLauncherConfig,
                                  final StandardSyncInput syncInput,
                                  final UUID connectionId) {

        System.out.println("trying activity");
        activity.run();
        System.out.println("yay!");
        return null;
    }

    @ActivityInterface
    public interface FailNTimesReplicationActivity {

        @ActivityMethod
        String run();

    }

}
