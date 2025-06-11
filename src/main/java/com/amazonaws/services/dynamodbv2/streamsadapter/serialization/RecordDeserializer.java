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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import software.amazon.awssdk.services.dynamodb.model.Identity;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RecordDeserializer extends JsonDeserializer<Record> {
    @Override
    public Record deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = p.getCodec().readTree(p);
        // Create a Record.Builder
        Record.Builder recordBuilder = Record.builder();
        // Set basic fields
        if (node.has(RecordObjectMapper.EVENT_ID)) {
            recordBuilder.eventID(node.get(RecordObjectMapper.EVENT_ID).asText());
        }
        if (node.has(RecordObjectMapper.EVENT_NAME)) {
            String eventName = node.get(RecordObjectMapper.EVENT_NAME).asText();
            recordBuilder.eventName(OperationType.fromValue(eventName));
        }
        if (node.has(RecordObjectMapper.EVENT_VERSION)) {
            recordBuilder.eventVersion(node.get(RecordObjectMapper.EVENT_VERSION).asText());
        }
        if (node.has(RecordObjectMapper.EVENT_SOURCE)) {
            recordBuilder.eventSource(node.get(RecordObjectMapper.EVENT_SOURCE).asText());
        }
        if (node.has(RecordObjectMapper.AWS_REGION)) {
            recordBuilder.awsRegion(node.get(RecordObjectMapper.AWS_REGION).asText());
        }
        // Handle userIdentity if present
        if (node.has(RecordObjectMapper.USER_IDENTITY)) {
            JsonNode identityNode = node.get(RecordObjectMapper.USER_IDENTITY);
            Identity.Builder identityBuilder = Identity.builder();
            if (identityNode.has("principalId")) {
                identityBuilder.principalId(identityNode.get("principalId").asText());
            }
            if (identityNode.has("type")) {
                identityBuilder.type(identityNode.get("type").asText());
            }
            recordBuilder.userIdentity(identityBuilder.build());
        }
        // Handle dynamodb section
        if (node.has(RecordObjectMapper.DYNAMODB)) {
            JsonNode dynamodbNode = node.get(RecordObjectMapper.DYNAMODB);
            StreamRecord.Builder streamRecordBuilder = StreamRecord.builder();
            // Parse StreamRecord fields
            if (dynamodbNode.has(RecordObjectMapper.APPROXIMATE_CREATION_DATE_TIME)) {
                long epochMilli = dynamodbNode.get(RecordObjectMapper.APPROXIMATE_CREATION_DATE_TIME).asLong();
                streamRecordBuilder.approximateCreationDateTime(java.time.Instant.ofEpochMilli(epochMilli));
            }
            // Parse Keys
            if (dynamodbNode.has(RecordObjectMapper.KEYS)) {
                JsonNode keysNode = dynamodbNode.get(RecordObjectMapper.KEYS);
                streamRecordBuilder.keys(parseAttributeValueMap(keysNode, p));
            }
            // Parse NewImage
            if (dynamodbNode.has(RecordObjectMapper.NEW_IMAGE)) {
                JsonNode newImageNode = dynamodbNode.get(RecordObjectMapper.NEW_IMAGE);
                streamRecordBuilder.newImage(parseAttributeValueMap(newImageNode, p));
            }
            // Parse OldImage
            if (dynamodbNode.has(RecordObjectMapper.OLD_IMAGE)) {
                JsonNode oldImageNode = dynamodbNode.get(RecordObjectMapper.OLD_IMAGE);
                streamRecordBuilder.oldImage(parseAttributeValueMap(oldImageNode, p));
            }
            // Parse SequenceNumber
            if (dynamodbNode.has(RecordObjectMapper.SEQUENCE_NUMBER)) {
                streamRecordBuilder.sequenceNumber(dynamodbNode.get(RecordObjectMapper.SEQUENCE_NUMBER).asText());
            }
            // Parse SizeBytes
            if (dynamodbNode.has(RecordObjectMapper.SIZE_BYTES)) {
                streamRecordBuilder.sizeBytes(dynamodbNode.get(RecordObjectMapper.SIZE_BYTES).asLong());
            }
            // Parse StreamViewType
            if (dynamodbNode.has(RecordObjectMapper.STREAM_VIEW_TYPE)) {
                String viewType = dynamodbNode.get(RecordObjectMapper.STREAM_VIEW_TYPE).asText();
                streamRecordBuilder.streamViewType(StreamViewType.fromValue(viewType));
            }
            recordBuilder.dynamodb(streamRecordBuilder.build());
        }
        return recordBuilder.build();
    }

    private Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> parseAttributeValueMap(
            JsonNode node, JsonParser parser) throws IOException {
        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> result = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String key = field.getKey();
            JsonNode valueNode = field.getValue();
            // Use the AttributeValueDeserializer to parse each value
            software.amazon.awssdk.services.dynamodb.model.AttributeValue value = parser.getCodec()
                    .treeToValue(valueNode, software.amazon.awssdk.services.dynamodb.model.AttributeValue.class);
            result.put(key, value);
        }
        return result;
    }
}
