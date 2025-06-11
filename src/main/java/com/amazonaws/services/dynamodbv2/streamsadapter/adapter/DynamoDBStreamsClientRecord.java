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
package com.amazonaws.services.dynamodbv2.streamsadapter.adapter;

import com.amazonaws.services.dynamodbv2.streamsadapter.serialization.RecordObjectMapper;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

@EqualsAndHashCode(callSuper = true)
@ToString
@Getter
@Slf4j
@SuppressWarnings("ParameterNumber")
public class DynamoDBStreamsClientRecord extends KinesisClientRecord {

    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private final Record record;

    protected DynamoDBStreamsClientRecord(
            String sequenceNumber,
            Instant approximateArrivalTimestamp,
            ByteBuffer data,
            String partitionKey,
            EncryptionType encryptionType,
            long subSequenceNumber,
            String explicitHashKey,
            boolean aggregated,
            Schema schema,
            Record record) {
        super(
                sequenceNumber,
                approximateArrivalTimestamp,
                data,
                partitionKey,
                encryptionType,
                subSequenceNumber,
                explicitHashKey,
                aggregated,
                schema);
        this.record = record;
    }

    public static DynamoDBStreamsClientRecord fromRecord(Record record) {
        return new DynamoDBStreamsClientRecord(
                record.dynamodb().sequenceNumber(),
                record.dynamodb().approximateCreationDateTime(),
                getData(record),
                record.dynamodb().sequenceNumber(),
                EncryptionType.NONE,
                0,
                null,
                false,
                null,
                record);
    }

    private static ByteBuffer getData(Record record) {
        ByteBuffer data;
        try {
            data = ByteBuffer.wrap(MAPPER.writeValueAsString(record).getBytes(DEFAULT_CHARSET));
        } catch (JsonProcessingException e) {
            final String errorMessage = "Failed to serialize stream record to JSON";
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
        return data;
    }
}
