package io.airbyte.integrations.bicycle.base.integration;

import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionRequest;
import com.inception.server.scheduler.api.JobExecutionStatus;

import java.util.Map;

public abstract class EventConnectorJobStatusHandler {
    protected Map<String,Boolean> eventConnectorStatusMap;
    public abstract void sendEventConnectorStatus(JobExecutionStatus jobExecutionStatus, JobExecutionRequest jobExecutionRequest, String response, String sourceId, AuthInfo authInfo);
    public void removeConnectorIdFromMap(String sourceId) {
        if(this.eventConnectorStatusMap.containsKey(sourceId)) {
            this.eventConnectorStatusMap.remove(sourceId);
        }
    }

    public void addConnectorIdToMap(String sourceId) {
        if(!this.eventConnectorStatusMap.containsKey(sourceId)) {
            this.eventConnectorStatusMap.put(sourceId,true);
        }
    }

    public boolean isEventConnectorRunning(String sourceId) {
        return this.eventConnectorStatusMap.containsKey(sourceId);
    }
}