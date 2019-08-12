/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;

public class TestUtil {

    /**
     * @return StreamId
     */
    public static String createTable(com.amazonaws.services.dynamodbv2.AmazonDynamoDB client, String tableName, Boolean withStream) {
        java.util.List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));

        java.util.List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput().withReadCapacityUnits(20L).withWriteCapacityUnits(20L);

        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName).withAttributeDefinitions(attributeDefinitions).withKeySchema(keySchema)
            .withProvisionedThroughput(provisionedThroughput);

        if (withStream) {
            StreamSpecification streamSpecification = new StreamSpecification();
            streamSpecification.setStreamEnabled(true);
            streamSpecification.setStreamViewType(StreamViewType.NEW_IMAGE);
            createTableRequest.setStreamSpecification(streamSpecification);
        }

        try {
            CreateTableResult result = client.createTable(createTableRequest);
            return result.getTableDescription().getLatestStreamArn();
        } catch (ResourceInUseException e) {
            return describeTable(client, tableName).getTable().getLatestStreamArn();
        }
    }

    public static void waitForTableActive(AmazonDynamoDB client, String tableName) throws IllegalStateException {
        Integer retries = 0;
        Boolean created = false;
        while (!created && retries < 100) {
            DescribeTableResult result = describeTable(client, tableName);
            created = result.getTable().getTableStatus().equals("ACTIVE");
            if (created) {
                return;
            } else {
                retries++;
                try {
                    Thread.sleep(500 + 100 * retries);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        }
        throw new IllegalStateException("Table not active!");
    }

    public static void updateTable(AmazonDynamoDBClient client, String tableName, Long readCapacity, Long writeCapacity) {
        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput().withReadCapacityUnits(readCapacity).withWriteCapacityUnits(writeCapacity);

        UpdateTableRequest updateTableRequest = new UpdateTableRequest().withTableName(tableName).withProvisionedThroughput(provisionedThroughput);

        client.updateTable(updateTableRequest);
    }

    public static DescribeTableResult describeTable(AmazonDynamoDB client, String tableName) {
        return client.describeTable(new DescribeTableRequest().withTableName(tableName));
    }

    public static StreamDescription describeStream(AmazonDynamoDBStreamsClient client, String streamArn, String lastEvaluatedShardId) {
        DescribeStreamResult result = client.describeStream(new DescribeStreamRequest().withStreamArn(streamArn).withExclusiveStartShardId(lastEvaluatedShardId));
        return result.getStreamDescription();
    }

    public static ScanResult scanTable(AmazonDynamoDB client, String tableName) {
        return client.scan(new ScanRequest().withTableName(tableName));
    }

    public static QueryResult queryTable(AmazonDynamoDB client, String tableName, String partitionKey) {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
        expressionAttributeValues.put(":v_id", new AttributeValue().withN(partitionKey));

        QueryRequest queryRequest = new QueryRequest().withTableName(tableName).withKeyConditionExpression("Id = :v_id").withExpressionAttributeValues(expressionAttributeValues);

        return client.query(queryRequest);
    }

    public static void putItem(AmazonDynamoDB client, String tableName, String id, String val) {
        java.util.Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("Id", new AttributeValue().withN(id));
        item.put("attribute-1", new AttributeValue().withS(val));

        PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
        client.putItem(putItemRequest);
    }

    public static void putItem(AmazonDynamoDB client, String tableName, java.util.Map<String, AttributeValue> items) {
        PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName).withItem(items);
        client.putItem(putItemRequest);
    }

    public static void updateItem(AmazonDynamoDB client, String tableName, String id, String val) {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", new AttributeValue().withN(id));

        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(new AttributeValue().withS(val));
        attributeUpdates.put("attribute-2", update);

        UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(tableName).withKey(key).withAttributeUpdates(attributeUpdates);
        client.updateItem(updateItemRequest);
    }

    public static void deleteItem(AmazonDynamoDB client, String tableName, String id) {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", new AttributeValue().withN(id));

        DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(tableName).withKey(key);
        client.deleteItem(deleteItemRequest);
    }

    public static String getShardIterator(AmazonDynamoDBStreamsClient client, String streamArn, String shardId) {
        GetShardIteratorResult result =
            client.getShardIterator(new GetShardIteratorRequest().withStreamArn(streamArn).withShardId(shardId).withShardIteratorType(ShardIteratorType.TRIM_HORIZON));
        return result.getShardIterator();
    }

    public static GetRecordsResult getRecords(AmazonDynamoDBStreamsClient client, String shardIterator) {
        GetRecordsResult result = client.getRecords(new GetRecordsRequest().withShardIterator(shardIterator));
        return result;
    }

}
