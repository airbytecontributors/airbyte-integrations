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
        return "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJST0xFIjoiQVBJIiwic3ViIjoiSU5GT1NFQy01OTMiLCJPUkdfSUQiOiI3NiIsImlzcyI6IjYzOWYyOGY3LTg4YzYtNGFkOC1hMGIiLCJleHAiOjE3MDQ1MzIzNzQsImlhdCI6MTcwMzMyMjc3NCwiVEVOQU5UIjoidGJyLWNlZDI5NzNkLWZiMDctNGNmNy05NzdjLTZiYzM1MWVlZmE1ZiIsImp0aSI6IjViNzhiNTQ5LTVlYjktNGRkNC05ZjIifQ.U0BqVKDlSN6F4lxfOMASypn8s1p5qaN59NXrjhpwcqQ";
    }

    @Override
    public String getTenantCode() {
        return null;
    }

    @Override
    public String getTenantId() {
        return "tbr-ced2973d-fb07-4cf7-977c-6bc351eefa5f";
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
