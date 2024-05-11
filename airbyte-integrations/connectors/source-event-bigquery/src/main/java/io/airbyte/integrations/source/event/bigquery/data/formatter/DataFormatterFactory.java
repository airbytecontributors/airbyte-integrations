package io.airbyte.integrations.source.event.bigquery.data.formatter;

/**
 * @author sumitmaheshwari
 * Created on 11/10/2023
 */
public class DataFormatterFactory {

   public static DataFormatter getDataFormatter(DataFormatterType formatterType,
                                                DataFormatterConfig dataFormatterConfig) {

        if (formatterType.equals(DataFormatterType.GoogleAnalytics4)) {
            return new GoogleAnalyticsV4DataFormatter(dataFormatterConfig);
        }

        return null;
    }

}
