package io.airbyte.integrations.acceptance_tests.destination;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"configPath"})
public class InputConfiguration {

  String configPath;
  String failedCheckConfigPath;
  String connectorImageName;
  String testHelperImageName;

  public String getConfigPath() {
    return configPath;
  }

  public void setConfigPath(String configPath) {
    this.configPath = configPath;
  }

  public String getFailedCheckConfigPath() {
    return failedCheckConfigPath;
  }

  public void setFailedCheckConfigPath(String failedCheckConfigPath) {
    this.failedCheckConfigPath = failedCheckConfigPath;
  }

  public String getConnectorImageName() {
    return connectorImageName;
  }

  public void setConnectorImageName(String connectorImageName) {
    this.connectorImageName = connectorImageName;
  }

  public String getTestHelperImageName() {
    return testHelperImageName;
  }

  public void setTestHelperImageName(String testHelperImageName) {
    this.testHelperImageName = testHelperImageName;
  }
}
