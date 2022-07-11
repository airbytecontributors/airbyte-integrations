package io.airbyte.integrations.bicycle.base.integration;

import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionRequest;
import com.inception.server.scheduler.api.JobExecutionStatus;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class EventConnectorStatusInitiator {
    EventConnectorStatusListener eventConnectorStatusResponseHandler = null;
    JobExecutionRequest jobExecutionRequest;
    AtomicInteger numberOfThreadsRunning=new AtomicInteger(0);
    ScheduledExecutorService ses = null;
    public EventConnectorStatusInitiator(JobExecutionRequest jobExecutionRequest) {
        this.jobExecutionRequest = jobExecutionRequest;
    }

    public void setHandler(io.airbyte.integrations.bicycle.base.integration.EventConnectorStatusListener eventConnectorStatusResponseHandler) {
        this.eventConnectorStatusResponseHandler=eventConnectorStatusResponseHandler;
    }

    public void setScheduledExecutorService(ScheduledExecutorService ses) {
        this.ses = ses;
    }

    public void setNumberOfThreadsRunning(AtomicInteger numberOfThreadsRunning) {
        this.numberOfThreadsRunning = numberOfThreadsRunning;
    }

    public AtomicInteger getNumberOfThreadsRunning() {
        return numberOfThreadsRunning;
    }

    public void removeConnectorIdFromMap(String sourceId) {
        eventConnectorStatusResponseHandler.removeConnectorIdFromMap(sourceId);
    }

    public void sendStatus(JobExecutionStatus jobExecutionStatus, String response, String sourceId, AuthInfo authInfo) {
        eventConnectorStatusResponseHandler.sendEventConnectorStatus(jobExecutionStatus, jobExecutionRequest, response, sourceId, authInfo);
    }

    public ScheduledExecutorService getSchedulesExecutorService() {
        return ses;
    }
}
