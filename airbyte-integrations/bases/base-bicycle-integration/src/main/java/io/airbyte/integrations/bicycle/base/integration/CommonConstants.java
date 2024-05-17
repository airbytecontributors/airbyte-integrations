package io.airbyte.integrations.bicycle.base.integration;

import ai.apptuit.metrics.client.TagEncodedMetricName;

/**
 * @author sumitmaheshwari
 * Created on 21/04/2023
 */
public class CommonConstants {

  public static final String CONNECTOR_IN_TIMESTAMP = "bicycle.connectorInTime";

  public static final TagEncodedMetricName CONNECTOR_RECORDS_PULL_METRIC = TagEncodedMetricName.decode("integration_connector_records_pull_time");
  public static final TagEncodedMetricName CONNECTOR_RECORDS_ITERATE_METRIC = TagEncodedMetricName.decode("integration_connector_records_iterate_time");

  public static final TagEncodedMetricName CONNECTOR_USER_SERVICE_RULES_DOWNLOAD = TagEncodedMetricName.decode("integration_connector_rules_download_time");
  public static final TagEncodedMetricName CONNECTOR_CONVERT_RECORDS_RAW_EVENTS = TagEncodedMetricName.decode("integration_connector_convert_raw_events_time");
  public static final TagEncodedMetricName CONNECTOR_PROCESS_RAW_EVENTS = TagEncodedMetricName.decode("integration_connector_process_raw_events_time");
  public static final TagEncodedMetricName CONNECTOR_PROCESS_RAW_EVENTS_WITH_RULES_DOWNLOAD = TagEncodedMetricName.decode("integration_connector_process_raw_events_with_rules_download_time");
  public static final TagEncodedMetricName CONNECTOR_PUBLISH_EVENTS = TagEncodedMetricName.decode("integration_connector_publish_raw_events_time");
  public static final String CONNECTOR_LAG_STRING = "integration_connector_lag";
  public static final TagEncodedMetricName CONNECTOR_LAG = TagEncodedMetricName
          .decode(CONNECTOR_LAG_STRING);

}
