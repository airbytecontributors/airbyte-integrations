/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@ActivityInterface
public interface ForceCancellationActivity {

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class ForceCancellationInput {

    // private UUID connectionId;

  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class ForceCancellationOutput {

    private boolean cancelled;

  }

  /**
   * Delete a connection
   */
  @ActivityMethod
  ForceCancellationOutput forceCancellation(ForceCancellationInput input);

}
