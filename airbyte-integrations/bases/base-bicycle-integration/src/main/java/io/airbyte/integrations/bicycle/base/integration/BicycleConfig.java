package io.airbyte.integrations.bicycle.base.integration;

import com.inception.server.auth.api.SystemAuthenticator;

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
    private String tenantId;
    private SystemAuthenticator systemAuthenticator;
    private boolean isOnPremDeployment;

    public BicycleConfig(String serverURL, String token, String connectorId, String uniqueIdentifier, String tenantId, SystemAuthenticator systemAuthenticator, boolean isOnPremDeployment) {
        this.serverURL = serverURL;
        this.token = token;
        this.connectorId = connectorId;
        this.uniqueIdentifier = uniqueIdentifier;
        this.eventURL = serverURL + "/api/ingester/events";
        this.tenantId = tenantId;
        this.systemAuthenticator = systemAuthenticator;
        this.isOnPremDeployment = isOnPremDeployment;
    }

    public String getServerURL() {
        return serverURL;
    }

    public String getToken() {
        return isOnPremDeployment ? token : this.systemAuthenticator.authenticate(tenantId).getToken();
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
     public String getTenantId() {
        return tenantId;
     }
}
