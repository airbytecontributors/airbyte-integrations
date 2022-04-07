/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.workers.helper.ConnectionHelper;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ForceCancellationActivityImpl implements ForceCancellationActivity {

  private final ConnectionHelper connectionHelper;

  @Override
  public ForceCancellationOutput forceCancellation(final ForceCancellationInput input) {
    // try {
    // connectionHelper.deleteConnection(input.getConnectionId());
    // } catch (final JsonValidationException | ConfigNotFoundException | IOException e) {
    // throw new RetryableException(e);
    // }
    return new ForceCancellationOutput(true);
  }

}
