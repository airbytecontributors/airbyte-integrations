import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.bicycle.base.integration.BicycleAuthInfo;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.source.kafka.BicycleConsumer;
import io.airbyte.integrations.source.kafka.KafkaSource;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.models.converter.BicycleEventsResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.bicycle.server.event.mapping.constants.OTELConstants.TENANT_ID;

public class KafkaSourceTest {

    static BicycleConsumer bicycleConsumer;
    static JsonNode config;
    static ConfiguredAirbyteCatalog catalog;
    private static BicycleAuthInfo authInfo;
    private static EventSourceInfo eventSourceInfo;
    private static BicycleConfig bicycleConfig;
    private static KafkaSource kafkaSource;

    @BeforeAll
    public static void setupBicycleConsumer() {
        String serverURL =  "https://api.dev.bicycle.io";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJBbnVyYWdCYWpwYWkiLCJPUkdfSUQiOiIyIiwiaXNzIjoiYWRtaW4iLCJpYXQiOjE2MDQ2NDYyNjgsIlRFTkFOVCI6IjY1ZTFlNTQxLWFhNTMtNDkyMi05MmJmLWJmNmM5NDViOTdjOCIsImp0aSI6ImM3OTBjZWVmLTU5ZTYtNGQwZC1iNmYifQ.FjRFA6uI8ARJdXn0wc3LTPfdzs5Yboyhm51YR7F41GI";
        String connectorId = "c_connector_stream:776cdc59-06da-4034-83ed-3054142ce3e1";
        String eventSourceType= "EVENT";

        Map<String, Long> totalRecordsRead = null;
        bicycleConfig = new BicycleConfig(serverURL, token, connectorId, uniqueIdentifier);
        authInfo = new BicycleAuthInfo(bicycleConfig.getToken(), TENANT_ID);
        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);

        config = null;
        catalog= new ConfiguredAirbyteCatalog();

        String consumerThreadId = UUID.randomUUID().toString();
        kafkaSource=new KafkaSource();
        kafkaSource.setBicycleEventProcessor(bicycleConfig);
        bicycleConsumer = new BicycleConsumer(consumerThreadId, totalRecordsRead, bicycleConfig, config, catalog,authInfo,eventSourceInfo,new KafkaSource());

    }

    @Test
    public void testConvertRecordsToRawEvents() {

        List<ConsumerRecord<String,JsonNode>> records=new ArrayList<>();

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("TestKey","TestValue");
        records.add(new ConsumerRecord<String, JsonNode>("Test",0, 0,"Key",(JsonNode) node));

        List<RawEvent> rawEventsFromConnector = new KafkaSource().convertRecordsToRawEvents(records);

        List<RawEvent> rawEventsExpected = new ArrayList<>();
        JsonRawEvent jsonRawEvent = new JsonRawEvent(node.toString());
        rawEventsExpected.add(jsonRawEvent);
        rawEventsExpected.get(0).getRawEventObject();
        Assertions.assertEquals(rawEventsExpected.get(0).getRawEventObject(),rawEventsFromConnector.get(0).getRawEventObject());
    };

    @Test
    public void testRawEventsToBicycleEvents() {
        List<ConsumerRecord<String,JsonNode>> records=new ArrayList<>();

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("TestKey","TestValue");
        records.add(new ConsumerRecord<String, JsonNode>("Test",0, 0,"Key",(JsonNode) node));
        List<RawEvent> rawEventsFromConnector = kafkaSource.convertRecordsToRawEvents(records);
        BicycleEventsResult biycleEvents = kafkaSource.convertRawEventsToBicycleEvents(authInfo, eventSourceInfo, rawEventsFromConnector);
        Assertions.assertEquals(biycleEvents.getUnmatchedBicycleEvents().getEvents(0).getData(1).getValue().getStringValue(),node.toString());
    }

    @Test
    public void testPublishEvents() {
        List<ConsumerRecord<String,JsonNode>> records=new ArrayList<>();

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("TestKey","TestValue");
        records.add(new ConsumerRecord<String, JsonNode>("Test",0, 0,"Key",(JsonNode) node));
        List<RawEvent> rawEventsFromConnector = kafkaSource.convertRecordsToRawEvents(records);
        BicycleEventsResult biycleEvents = kafkaSource.convertRawEventsToBicycleEvents(authInfo, eventSourceInfo, rawEventsFromConnector);
        boolean testResult = kafkaSource.publishEvents(authInfo, eventSourceInfo,biycleEvents);
        Assertions.assertEquals(testResult, true);
    }

    @Test
    public void testGetNumberOfRecordsToBeReturnedBasedOnSamplingRate() {
        int numberOfRecords = 1;
        int samplingRate = 100;
        int noOfRecords = bicycleConsumer.getNumberOfRecordsToBeReturnedBasedOnSamplingRate(numberOfRecords,samplingRate);
        int expectedValue = ((noOfRecords * samplingRate) / 100)==0 ? 1 : ((noOfRecords * samplingRate) / 100);
        Assertions.assertEquals(noOfRecords, expectedValue);
    }
}
