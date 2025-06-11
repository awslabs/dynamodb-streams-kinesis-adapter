/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Identity;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

import java.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class RecordObjectMapperTest {
    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    private static final String TEST_STRING = "TestString";

    private static final Instant TEST_DATE = new Date(1156377600 /* EC2 Announced */).toInstant();

    private Record dynamoRecord;

    private final String eventID = "0d391139-b73a-44e3-b1f3-671be3d9973a";

    private software.amazon.awssdk.services.kinesis.model.Record kinesisRecord;

    @BeforeEach
    public void setUpTest() {
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("hashKey", AttributeValue.builder().s("hashKeyValue").build());
        Map<String, AttributeValue> newImage = new HashMap<String, AttributeValue>(key);
        newImage.put("newAttributeKey", AttributeValue.builder().s("someValue").build());

        dynamoRecord = Record.builder()
                .awsRegion("us-east-1")
                .eventID(eventID)
                .eventSource("aws:dynamodb")
                .eventVersion("1.1")
                .eventName(OperationType.MODIFY)
                .dynamodb(StreamRecord.builder()
                        .approximateCreationDateTime(TEST_DATE)
                        .keys(key)
                        .oldImage(new HashMap<>(key))
                        .newImage(newImage)
                        .sizeBytes(Long.MAX_VALUE)
                        .sequenceNumber(TEST_STRING)
                        .streamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .build()).build();
    }

    @Test
    public void testDDBRecordSerialization() throws JsonProcessingException {
        String serializedDDBRecord = MAPPER.writeValueAsString(dynamoRecord);
        String expectedSerializedRecord = "{\"eventID\":\"0d391139-b73a-44e3-b1f3-671be3d9973a\",\"eventName\":\"MODIFY\",\"eventVersion\":\"1.1\",\"eventSource\":\"aws:dynamodb\",\"awsRegion\":\"us-east-1\",\"dynamodb\":{\"ApproximateCreationDateTime\":1156377600,\"Keys\":{\"hashKey\":{\"S\":\"hashKeyValue\"}},\"NewImage\":{\"newAttributeKey\":{\"S\":\"someValue\"},\"hashKey\":{\"S\":\"hashKeyValue\"}},\"OldImage\":{\"hashKey\":{\"S\":\"hashKeyValue\"}},\"SequenceNumber\":\"TestString\",\"SizeBytes\":9223372036854775807,\"StreamViewType\":\"NEW_AND_OLD_IMAGES\"}}";
        Assertions.assertEquals(expectedSerializedRecord, serializedDDBRecord);
    }

    /**
     * See {resources/complex_structure_expected.json for the expected structure
     * @throws JsonProcessingException
     * @throws Exception
     */
    @Test
    public void testDDBRecordSerializationForComplexStructure() throws Exception {
        dynamoRecord = getComplexRecord();
                
        // Read expected JSON from file
        String expectedJSON = new String(java.nio.file.Files.readAllBytes(
            java.nio.file.Paths.get(
                    "src/test/java/com/amazonaws/services/dynamodbv2/streamsadapter/serialization/complex_structure_expected.json")));

        // Remove whitespace for comparison (since the file has formatting)
        ObjectMapper plainMapper = new ObjectMapper();
        JsonNode expectedNode = plainMapper.readTree(expectedJSON);
        JsonNode actualNode = plainMapper.readTree(MAPPER.writeValueAsString(dynamoRecord));
        
        Assertions.assertEquals(expectedNode, actualNode);
    }

    @Test
    public void testDDBRecordSerializationDeserializationRoundTrip() throws Exception {
        dynamoRecord = getComplexRecord();

        Record roundTripRecord = MAPPER.readValue(MAPPER.writeValueAsString(dynamoRecord), Record.class);
        Assertions.assertEquals(dynamoRecord, roundTripRecord);
    }

    private Record getComplexRecord() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("anonymized-id").build());
        Map<String, AttributeValue> newImage = new HashMap<>();
        newImage.put("id", AttributeValue.builder().s("anonymized-id").build());
        newImage.put("accountId", AttributeValue.builder().s("anonymized-account-id").build());
        List<AttributeValue> accountTypeList = new ArrayList<>();
        accountTypeList.add(AttributeValue.builder().s("account-type").build());
        accountTypeList.add(AttributeValue.builder().ns(Arrays.asList("1", "2")).build());
        newImage.put("accountType", AttributeValue.builder().l(accountTypeList).build());
        newImage.put("adProduct", AttributeValue.builder().s("PRODUCT_TYPE").build());

        // Add NULL type
        newImage.put("nullValue", AttributeValue.builder().nul(true).build());
        newImage.put("nonNullValue", AttributeValue.builder().nul(false).build());
        
        // Add Binary type
        byte[] binaryData = new byte[] {1, 2, 3, 4, 5};
        newImage.put("binaryValue", AttributeValue.builder().b(software.amazon.awssdk.core.SdkBytes.fromByteArray(binaryData)).build());
        
        // Add Binary Set types - empty and non-empty
        newImage.put("binarySetEmpty", AttributeValue.builder().bs(new ArrayList<>()).build());
        
        List<software.amazon.awssdk.core.SdkBytes> binarySetValues = new ArrayList<>();
        binarySetValues.add(software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[] {10, 20, 30}));
        binarySetValues.add(software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[] {40, 50, 60}));
        newImage.put("binarySetNonEmpty", AttributeValue.builder().bs(binarySetValues).build());

        // audit
        Map<String, AttributeValue> auditMap = new HashMap<>();
        auditMap.put("createdTime", AttributeValue.builder().n("1000000000000").build());
        auditMap.put("lastUpdatedByUserId", AttributeValue.builder().s("anonymized-user-id").build());
        auditMap.put("lastUpdatedSource", AttributeValue.builder().s("anonymized-source").build());
        auditMap.put("lastUpdatedTime", AttributeValue.builder().n("1000000000001").build());
        newImage.put("audit", AttributeValue.builder().m(auditMap).build());

        // bidSettings
        newImage.put("bidSettings", AttributeValue.builder().m(new HashMap<>()).build());
        newImage.put("bidSettingsEmptySS", AttributeValue.builder().ss(new ArrayList<>()).build());
        List<String> nonEmptyBidSettings = new ArrayList<>();
        nonEmptyBidSettings.add("bid-settings-1");
        newImage.put("bidSettingsNonEmptySS", AttributeValue.builder().ss(nonEmptyBidSettings).build());

        newImage.put("bidSettingsEmptyNS", AttributeValue.builder().ns(new ArrayList<>()).build());
        newImage.put("bidSettingsNonEmptyNS", AttributeValue.builder().ns(Arrays.asList("1", "2")).build());
        // budget
        Map<String, AttributeValue> budgetMap = new HashMap<>();
        // budget.budgetCaps
        List<AttributeValue> budgetCapsList = new ArrayList<>();
        Map<String, AttributeValue> budgetCap = new HashMap<>();
        Map<String, AttributeValue> monetaryBudget = new HashMap<>();
        Map<String, AttributeValue> amount = new HashMap<>();
        Map<String, AttributeValue> amountDetails = new HashMap<>();
        amountDetails.put("currencyCode", AttributeValue.builder().s("CUR").build());
        amountDetails.put("value", AttributeValue.builder().n("100").build());
        amount.put("amount", AttributeValue.builder().m(amountDetails).build());
        monetaryBudget.put("monetaryBudget", AttributeValue.builder().m(amount).build());
        Map<String, AttributeValue> recurrence = new HashMap<>();
        Map<String, AttributeValue> recurrenceDetails = new HashMap<>();
        recurrenceDetails.put("recurrenceType", AttributeValue.builder().s("RECURRENCE_TYPE").build());
        recurrence.put("recurrence", AttributeValue.builder().m(recurrenceDetails).build());
        budgetCap.putAll(monetaryBudget);
        budgetCap.putAll(recurrence);
        budgetCapsList.add(AttributeValue.builder().m(budgetCap).build());
        budgetMap.put("budgetCaps", AttributeValue.builder().l(budgetCapsList).build());

        // budget.budgetPacing
        List<AttributeValue> budgetPacingList = new ArrayList<>();
        Map<String, AttributeValue> budgetPacing = new HashMap<>();
        budgetPacing.put("pacingStrategy", AttributeValue.builder().s("PACING_STRATEGY").build());
        budgetPacingList.add(AttributeValue.builder().m(budgetPacing).build());
        budgetMap.put("budgetPacing", AttributeValue.builder().l(budgetPacingList).build());
        budgetMap.put("type", AttributeValue.builder().s("BUDGET_TYPE").build());
        newImage.put("budget", AttributeValue.builder().m(budgetMap).build());

        // globalContext
        newImage.put("globalContext", AttributeValue.builder().s("CONTEXT").build());

        // internalTags
        List<AttributeValue> internalTagsList = new ArrayList<>();
        Map<String, AttributeValue> internalTag = new HashMap<>();
        internalTag.put("name", AttributeValue.builder().s("tag-name").build());
        internalTag.put("value", AttributeValue.builder().s("tag-value").build());
        internalTagsList.add(AttributeValue.builder().m(internalTag).build());
        newImage.put("internalTags", AttributeValue.builder().l(internalTagsList).build());

        // isGlobal
        newImage.put("isGlobal", AttributeValue.builder().bool(false).build());

        // isTest
        newImage.put("isTest", AttributeValue.builder().bool(false).build());

        // lastUpdatedBy
        newImage.put("lastUpdatedBy", AttributeValue.builder().s("anonymized-updater").build());

        // lastUpdatedTimeMillis
        newImage.put("lastUpdatedTimeMillis", AttributeValue.builder().n("1000000000002").build());

        // marketplaceIds
        List<AttributeValue> marketplaceIdsList = new ArrayList<>();
        marketplaceIdsList.add(AttributeValue.builder().s("anonymized-marketplace-id").build());
        newImage.put("marketplaceIds", AttributeValue.builder().l(marketplaceIdsList).build());

        // name
        newImage.put("name", AttributeValue.builder().s("anonymized-name").build());

        // optimizations
        Map<String, AttributeValue> optimizationsMap = new HashMap<>();
        List<AttributeValue> bidAdjustmentsList = new ArrayList<>();

        // First bid adjustment
        Map<String, AttributeValue> bidAdjustment1 = new HashMap<>();
        bidAdjustment1.put("bidAdjustmentPercentage", AttributeValue.builder().n("10").build());
        bidAdjustment1.put("bidAdjustmentType", AttributeValue.builder().s("ADJUSTMENT_TYPE_1").build());
        bidAdjustmentsList.add(AttributeValue.builder().m(bidAdjustment1).build());

        // Second bid adjustment
        Map<String, AttributeValue> bidAdjustment2 = new HashMap<>();
        bidAdjustment2.put("bidAdjustmentPercentage", AttributeValue.builder().n("20").build());
        bidAdjustment2.put("bidAdjustmentType", AttributeValue.builder().s("ADJUSTMENT_TYPE_2").build());
        bidAdjustmentsList.add(AttributeValue.builder().m(bidAdjustment2).build());

        // Third bid adjustment
        Map<String, AttributeValue> bidAdjustment3 = new HashMap<>();
        bidAdjustment3.put("bidAdjustmentPercentage", AttributeValue.builder().n("30").build());
        bidAdjustment3.put("bidAdjustmentType", AttributeValue.builder().s("ADJUSTMENT_TYPE_3").build());
        bidAdjustmentsList.add(AttributeValue.builder().m(bidAdjustment3).build());
        optimizationsMap.put("bidAdjustments", AttributeValue.builder().l(bidAdjustmentsList).build());
        optimizationsMap.put("bidStrategy", AttributeValue.builder().s("BID_STRATEGY").build());
        optimizationsMap.put("isPremiumBidEnabled", AttributeValue.builder().bool(false).build());
        newImage.put("optimizations", AttributeValue.builder().m(optimizationsMap).build());

        // startDateTime
        newImage.put("startDateTime", AttributeValue.builder().n("1000000000003").build());

        // state
        newImage.put("state", AttributeValue.builder().s("STATE").build());

        // status
        Map<String, AttributeValue> statusMap = new HashMap<>();
        statusMap.put("deliveryReasons", AttributeValue.builder().l(new ArrayList<>()).build());
        statusMap.put("deliveryStatus", AttributeValue.builder().s("STATUS").build());
        newImage.put("status", AttributeValue.builder().m(statusMap).build());

        // tags
        newImage.put("tags", AttributeValue.builder().l(new ArrayList<>()).build());

        // targetingSetting
        newImage.put("targetingSetting", AttributeValue.builder().s("TARGETING").build());

        // version
        Map<String, AttributeValue> versionMap = new HashMap<>();
        versionMap.put("objectVersion", AttributeValue.builder().n("1").build());
        newImage.put("version", AttributeValue.builder().m(versionMap).build());

        // Generate a random event ID
        String eventID = "anonymized-event-id";

        // Now create the Record object
        dynamoRecord = Record.builder()
                .awsRegion("region-1")
                .eventID(eventID)
                .eventSource("aws:service")
                .eventVersion("1.0")
                .eventName(OperationType.MODIFY)
                .dynamodb(StreamRecord.builder()
                        .approximateCreationDateTime(TEST_DATE)
                        .keys(key)
                        .oldImage(new HashMap<>(key))
                        .newImage(newImage)
                        .sizeBytes(1000L)
                        .sequenceNumber("anonymized-sequence")
                        .streamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .build())
                .userIdentity(Identity.builder()
                        .principalId("anonymized-principal")
                        .type("anonymized-type")
                        .build())
                .build();
        return dynamoRecord;
    }
}
