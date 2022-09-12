package io.airbyte.integrations.bicycle.base.integration;

import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 11/05/2022
 */
public class BicycleConfig {

    private final String serverURL;
    private final String token;
    private final String USER_ID_SEPARATOR = "#@#@";
    private final String connectorId;
    private final String uniqueIdentifier;
    private final String eventURL;
    private final String metricStoreURL;
    private String tenantId;
    private SystemAuthenticator systemAuthenticator;
    private boolean isOnPremDeployment;
    private AuthInfo authInfo;
    private static final Logger LOGGER = LoggerFactory.getLogger(BicycleConfig.class);

    public BicycleConfig(String serverURL, String metricStoreURL,String token, String connectorId,String uniqueIdentifier, String tenantId, SystemAuthenticator systemAuthenticator, boolean isOnPremDeployment) {
        this.serverURL = serverURL;
        this.token = token;
        this.connectorId = connectorId;
        this.uniqueIdentifier = uniqueIdentifier;
        this.eventURL = serverURL + "/api/ingester/events";
        this.metricStoreURL = metricStoreURL;
        this.tenantId = tenantId;
        this.systemAuthenticator = systemAuthenticator;
        this.isOnPremDeployment = isOnPremDeployment;
        if (this.isOnPremDeployment == true) {
            String setProperty = System.setProperty("isConnectorMode", "true");
            authInfo = new BicycleAuthInfo(this.token, this.tenantId);
            String s = System.setProperty("metric.client.token", this.token);
        }
    }

    public String getServerURL() {
        return serverURL;
    }

    public AuthInfo getAuthInfo() {
        AuthInfo localVariableAuthInfo;
        if (isOnPremDeployment) {
            localVariableAuthInfo = authInfo;
        }
        else {
            localVariableAuthInfo = this.systemAuthenticator.authenticate(tenantId);
        }
//        LOGGER.info("TenantId: {} token {}", tenantId, localVariableAuthInfo.getToken());
        return localVariableAuthInfo;
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

    public String getMetricStoreURL() {
        return metricStoreURL;
    }
}
