package io.airbyte.integrations.standardtest.source;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;

public abstract class EventSourceAcceptanceTest extends SourceAcceptanceTest {
    @Test
    public void testPreview() throws Exception {
        final ConfiguredAirbyteCatalog catalog = withFullRefreshSyncModes(getConfiguredCatalog());
        final List<AirbyteMessage> allMessages = runRead(catalog);

        assertFalse(filterRecords(allMessages).isEmpty(), "Expected a full refresh sync to produce preview records");
        assertFullRefreshMessages(allMessages);
    }

}
