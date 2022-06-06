package io.airbyte.integrations.bicycle.base.integration;

/**
 * @author sumitmaheshwari
 * Created on 11/05/2022
 */
public class BicycleConfig {

    private final String serverURL;
    private final String token;
    private final String connectorId;
    private final String uniqueIdentifier;
    private final String eventURL;

    public BicycleConfig(String serverURL, String token, String connectorId, String uniqueIdentifier) {
        this.serverURL = serverURL;
        this.token = token;
        this.connectorId = connectorId;
        this.uniqueIdentifier = uniqueIdentifier;
        this.eventURL = serverURL + "/api/ingester/events";
    }

    public String getServerURL() {
        return serverURL;
    }

    public String getToken() {
        return token;
    }

    public String getConnectorId() {
        return connectorId;
    }

    public String getEventURL() {
        return eventURL;
    }

    public String getUniqueIdentifier() {
        return uniqueIdentifier;
    }
}
