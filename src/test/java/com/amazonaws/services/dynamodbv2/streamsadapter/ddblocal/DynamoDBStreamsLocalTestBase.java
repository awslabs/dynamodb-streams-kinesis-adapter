package com.amazonaws.services.dynamodbv2.streamsadapter.ddblocal;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;

/**
 * Shared setup for DynamoDB Local integration tests.
 * Starts an in-memory DynamoDB Local instance and creates shared clients.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DynamoDBStreamsLocalTestBase {

    protected static final String PORT = "8000";
    protected static final URI LOCAL_ENDPOINT = URI.create("http://localhost:" + PORT);
    protected static final Region REGION = Region.US_EAST_1;
    protected static final StaticCredentialsProvider CREDENTIALS =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("fakeAccessKey", "fakeSecretKey"));

    protected DynamoDBProxyServer server;
    protected DynamoDbClient dynamoDbClient;
    protected DynamoDbStreamsClient streamsClient;
    protected AmazonDynamoDBStreamsAdapterClient adapterClient;

    @BeforeAll
    public void setup() throws Exception {
        server = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", PORT});
        server.start();

        dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(LOCAL_ENDPOINT)
                .region(REGION)
                .credentialsProvider(CREDENTIALS)
                .build();

        streamsClient = DynamoDbStreamsClient.builder()
                .endpointOverride(LOCAL_ENDPOINT)
                .region(REGION)
                .credentialsProvider(CREDENTIALS)
                .build();

        adapterClient = new AmazonDynamoDBStreamsAdapterClient(streamsClient, REGION);
    }

    @AfterAll
    public void teardown() throws Exception {
        dynamoDbClient.close();
        streamsClient.close();
        adapterClient.close();
        server.stop();
    }

    protected String createTableWithStream(String tableName) {
        dynamoDbClient.createTable(CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id").attributeType(ScalarAttributeType.S).build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                .streamSpecification(StreamSpecification.builder()
                        .streamEnabled(true).streamViewType(StreamViewType.NEW_AND_OLD_IMAGES).build())
                .build());
        dynamoDbClient.waiter().waitUntilTableExists(request -> request.tableName(tableName));
        return dynamoDbClient.describeTable(request -> request.tableName(tableName))
                .table().latestStreamArn();
    }

    protected void putItem(String tableName, String id, String value) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s(id).build());
        item.put("data", AttributeValue.builder().s(value).build());
        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build());
    }

    protected String getShardIteratorFromStreams(String streamArn) {
        software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse desc =
                streamsClient.describeStream(software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest
                        .builder().streamArn(streamArn).build());
        String shardId = desc.streamDescription().shards().get(0).shardId();
        return streamsClient.getShardIterator(
                software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest.builder()
                        .streamArn(streamArn)
                        .shardId(shardId)
                        .shardIteratorType("TRIM_HORIZON")
                        .build()).shardIterator();
    }
}
