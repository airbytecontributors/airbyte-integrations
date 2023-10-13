package io.airbyte.integrations.bicycle.base.integration;

import com.inception.server.auth.api.AuthorizeUser;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.auth.model.AuthType;
import com.inception.server.auth.model.Principal;

/**
 * @author sumitmaheshwari
 * Created on 13/10/2023
 */
public class DevAuthInfo implements AuthInfo {

    @Override
    public Principal getPrincipal() {
        return null;
    }

    @Override
    public String getToken() {
        return "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJST0xFIjoiQVBJIiwic3ViIjoiZXZlbnQtZGVtby1hcHAtMiIsIk9SR19JRCI6IjgwIiwiaXNzIjoiamF5YUBiaWN5Y2xlLmlvIiwiaWF0IjoxNjYzNTgyNjgwLCJURU5BTlQiOiJldnQtZmJiOTY3YWQtMjVmMi00ZWVlLWIyZTUtZjUyYjA0N2JlMmVmIiwianRpIjoiZTQxMDhhNDMtYjVmNC00ZmRkLTg5NiJ9.wC6lMnpMvlNMvvyI_TPP4vzHRgPQstu0IUSpkD5aIPg";
    }

    @Override
    public String getTenantCode() {
        return null;
    }

    @Override
    public String getTenantId() {
        return "evt-fbb967ad-25f2-4eee-b2e5-f52b047be2ef";
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
        return new String[] {AuthorizeUser.READ_WRITE};
    }
}
