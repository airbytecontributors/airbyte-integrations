package io.airbyte.integrations.bicycle.base.integration;

import com.inception.server.auth.model.AuthInfo;
import com.inception.server.auth.model.AuthType;
import com.inception.server.auth.model.Principal;

public class BicycleAuthInfo implements AuthInfo {
    private final String token;
    private final String tenantId;

    public BicycleAuthInfo(String token, String tenantId) {
        this.token = token;
        this.tenantId = tenantId;
    }

    @Override
    public Principal getPrincipal() {
        return null;
    }

    @Override
    public String getToken() {
        return token;
    }

    @Override
    public String getTenantCode() {
        return null;
    }

    @Override
    public String getTenantId() {
        return tenantId;
    }

    @Override
    public String getAccountId() {
        return null;
    }

    @Override
    public AuthType getAuthType() {
        return AuthType.BEARER;
    }

    @Override
    public String[] getRoles() {
        String[] roles = new String[1];
        roles[0] = "AGENT";
        return roles;
    }
}
