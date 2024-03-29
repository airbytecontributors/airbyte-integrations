package io.airbyte.integrations.bicycle.base.integration;

import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionRequest;
import com.inception.server.scheduler.api.JobExecutionStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class EventConnectorJobStatusHandler {
//    using this hashmap to reuse Event Connector instance for connector streams
    private Map<String, BaseEventConnector> connectorIdToEventConnectorInstance = new ConcurrentHashMap<>();
    public abstract void sendEventConnectorStatus(JobExecutionStatus jobExecutionStatus, JobExecutionRequest jobExecutionRequest, String response, String sourceId, int recordsRead,AuthInfo authInfo);
    public Map<String, BaseEventConnector> getConnectorIdToEventConnectorInstanceMap() {
        return connectorIdToEventConnectorInstance;
    }
}