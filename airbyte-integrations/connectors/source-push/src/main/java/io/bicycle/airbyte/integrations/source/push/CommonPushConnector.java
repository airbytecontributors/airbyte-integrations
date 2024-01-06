package io.bicycle.airbyte.integrations.source.push;

import com.fasterxml.jackson.databind.JsonNode;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of this connector is to serve the sync data for push based connectors as we don't
 * actually run push based connectors in our system.
 */

public class CommonPushConnector extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonPushConnector.class);


    public CommonPushConnector(SystemAuthenticator systemAuthenticator,
                               EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                               ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    protected int getTotalRecordsConsumed() {
        return 0;
    }

    public void stopEventConnector() {
        stopEventConnector("Successfully Stopped", JobExecutionStatus.success);
    }

    @Override
    public List<RawEvent> convertRecordsToRawEventsInternal(List<?> records) {
        return null;
    }

    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                         JsonNode state) throws InterruptedException, ExecutionException {
        return null;
    }

    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        LOGGER.info("Check the status");
        return new AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)
                .withMessage("Success");
    }

    public AirbyteCatalog discover(JsonNode config) throws Exception {
        LOGGER.info("Discover the common push connector");
        List<AirbyteStream> airbyteStreams = new ArrayList<>();
        AirbyteStream airbyteStream = new AirbyteStream().withName("dummy");
        airbyteStreams.add(airbyteStream);
        return new AirbyteCatalog().withStreams(airbyteStreams);

    }

    public AutoCloseableIterator<AirbyteMessage> doRead(
            JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) {
        return  null;
    }

}
