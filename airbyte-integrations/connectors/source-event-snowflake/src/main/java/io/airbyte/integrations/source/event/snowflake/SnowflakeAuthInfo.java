package io.airbyte.integrations.source.event.snowflake;

/**
 * @author sumitmaheshwari
 * Created on 04/04/2024
 */
public class SnowflakeAuthInfo {
    private String authType;
    private UserNamePassword userNamePassword;
    private OAuth auth;

    public SnowflakeAuthInfo(String authType, UserNamePassword userNamePassword) {
        this.authType = authType;
        this.userNamePassword = userNamePassword;
    }

    public SnowflakeAuthInfo(String authType, OAuth auth) {
        this.authType = authType;
        this.auth = auth;
    }

    public static class OAuth {
        private String clientId;
        private String clientSecret;
        private String accessToken;
        private String refreshToken;

        public OAuth(String clientId, String clientSecret, String accessToken, String refreshToken) {
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.accessToken = accessToken;
            this.refreshToken = refreshToken;
        }

        public String getClientId() {
            return clientId;
        }

        public String getClientSecret() {
            return clientSecret;
        }

        public String getAccessToken() {
            return accessToken;
        }

        public String getRefreshToken() {
            return refreshToken;
        }
    }

    public static class UserNamePassword {
        private String userName;
        private String password;

        public UserNamePassword(String userName, String password) {
            this.userName = userName;
            this.password = password;
        }

        public String getUserName() {
            return userName;
        }

        public String getPassword() {
            return password;
        }
    }

}
