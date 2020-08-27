package io.dataline.workers.protocol;

import io.dataline.config.SingerMessage;
import io.dataline.config.State;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

public class SingerMessageTracker implements Consumer<SingerMessage> {
  private final MutableLong recordCount;
  private final Mutable<State> outputState;
  private final UUID connectionId;

  public SingerMessageTracker(UUID connectionId) {
    this.connectionId = connectionId;
    this.recordCount = new MutableLong();
    this.outputState = new MutableObject<>();
  }

  @Override
  public void accept(SingerMessage record) {
    if (record.getType().equals(SingerMessage.Type.RECORD)) {
      recordCount.increment();
    }
    if (record.getType().equals(SingerMessage.Type.STATE)) {
      final State state = new State();
      state.setConnectionId(connectionId);
      state.setState(record);
      outputState.setValue(state);
    }
  }

  public long getRecordCount() {
    return recordCount.getValue();
  }

  public Optional<State> getOutputState() {
    return Optional.ofNullable(outputState.getValue());
  }
}
