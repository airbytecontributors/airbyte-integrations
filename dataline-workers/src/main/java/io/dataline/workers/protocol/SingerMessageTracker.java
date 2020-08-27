package io.dataline.workers.protocol;

import io.dataline.config.SingerMessage;
import io.dataline.config.State;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class SingerMessageTracker implements Consumer<SingerMessage> {
  private final AtomicLong recordCount;
  private final AtomicReference<State> outputState;
  private final UUID connectionId;

  public SingerMessageTracker(UUID connectionId) {
    this.connectionId = connectionId;
    this.recordCount = new AtomicLong();
    this.outputState = new AtomicReference<>();
  }

  @Override
  public void accept(SingerMessage record) {
    if (record.getType().equals(SingerMessage.Type.RECORD)) {
      recordCount.incrementAndGet();
    }
    if (record.getType().equals(SingerMessage.Type.STATE)) {
      final State state = new State();
      state.setConnectionId(connectionId);
      state.setState(record);
      outputState.set(state);
    }
  }

  public long getRecordCount() {
    return recordCount.get();
  }

  public Optional<State> getOutputState() {
    return Optional.ofNullable(outputState.get());
  }
}
