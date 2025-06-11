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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Identity;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;

public class RecordObjectMapper extends ObjectMapper {
    /** Identity field for user information in the record. */
    public static final String USER_IDENTITY = "userIdentity";

    /** Field name for the old image in DynamoDB Stream record. */
    public static final String OLD_IMAGE = "OldImage";

    /** Field name for the new image in DynamoDB Stream record. */
    public static final String NEW_IMAGE = "NewImage";

    /** Field name for stream view type. */
    public static final String STREAM_VIEW_TYPE = "StreamViewType";

    /** Field name for sequence number. */
    public static final String SEQUENCE_NUMBER = "SequenceNumber";

    /** Field name for size in bytes. */
    public static final String SIZE_BYTES = "SizeBytes";

    /** Field name for record keys. */
    public static final String KEYS = "Keys";

    /** Field name for AWS region. */
    public static final String AWS_REGION = "awsRegion";

    /** Field name for DynamoDB section of the record. */
    public static final String DYNAMODB = "dynamodb";

    /** Field name for event ID. */
    public static final String EVENT_ID = "eventID";

    /** Field name for event name. */
    public static final String EVENT_NAME = "eventName";

    /** Field name for event source. */
    public static final String EVENT_SOURCE = "eventSource";

    /** Field name for event version. */
    public static final String EVENT_VERSION = "eventVersion";

    /** Field name for approximate creation date time. */
    public static final String APPROXIMATE_CREATION_DATE_TIME = "ApproximateCreationDateTime";

    private static final String MODULE = "custom";

    public RecordObjectMapper() {
        super();
        // Disable failing on empty beans
        this.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        SimpleModule module = new SimpleModule(MODULE, Version.unknownVersion());

        // Deal with (de)serializing of byte[].
        module.addSerializer(ByteBuffer.class, new ByteBufferSerializer());
        module.addDeserializer(ByteBuffer.class, new ByteBufferDeserializer());
        // Custom serializer and deserializer for AttributeValue
        module.addSerializer(AttributeValue.class, new AttributeValueSerializer());
        module.addDeserializer(AttributeValue.class, new AttributeValueDeserializer());

        // Add custom serializer for Instant to ensure it's serialized as milliseconds (integer)
        module.addSerializer(Instant.class, new InstantAsMillisSerializer());
        // Add custom deserializers for Record
        module.addDeserializer(Record.class, new RecordDeserializer());

        // Register the module
        this.registerModule(module);

        // Don't serialize things that are null
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        // We need to keep empty collections for proper AttributeValue serialization
        // but we'll handle them in our custom AttributeValueSerializer

        this.addMixIn(Identity.class, IdentityMixIn.class);
        this.addMixIn(AttributeValue.class, AttributeValueMixIn.class);
        this.addMixIn(Record.class, RecordMixIn.class);
        this.addMixIn(StreamRecord.class, StreamRecordMixIn.class);
    }

    /*
     * Serializers and Deserializer classes
     */
    private static class ByteBufferSerializer extends JsonSerializer<ByteBuffer> {
        @Override
        public void serialize(ByteBuffer value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException, JsonProcessingException {
            // value is never null, according to JsonSerializer contract
            jgen.writeBinary(value.array());
        }
    }


    private static class ByteBufferDeserializer extends JsonDeserializer<ByteBuffer> {
        @Override
        public ByteBuffer deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
                JsonProcessingException {
            // never called for null literal, according to JsonDeserializer contract
            return ByteBuffer.wrap(jp.getBinaryValue());
        }
    }


    private abstract static class RecordMixIn {
        @JsonProperty(AWS_REGION)
        public abstract String awsRegion();

        @JsonProperty(DYNAMODB)
        public abstract StreamRecord dynamodb();

        @JsonProperty(EVENT_ID)
        public abstract String eventID();

        @JsonProperty(EVENT_NAME)
        public abstract String eventName();

        @JsonProperty(EVENT_SOURCE)
        public abstract String eventSource();

        @JsonProperty(EVENT_VERSION)
        public abstract String eventVersion();
        @JsonProperty(USER_IDENTITY)
        public abstract Identity userIdentity();
    }

    private abstract static class IdentityMixIn {
        @JsonProperty("principalId")
        public abstract String principalId();

        @JsonProperty("type")
        public abstract String type();
    }

    private abstract static class StreamRecordMixIn {
        @JsonProperty(SIZE_BYTES)
        public abstract Long sizeBytes();

        @JsonProperty(SEQUENCE_NUMBER)
        public abstract String sequenceNumber();

        @JsonProperty(STREAM_VIEW_TYPE)
        public abstract StreamViewType streamViewType();

        @JsonProperty(KEYS)
        public abstract Map<String, AttributeValue> keys();

        @JsonProperty(NEW_IMAGE)
        public abstract Map<String, AttributeValue> newImage();

        @JsonProperty(OLD_IMAGE)
        public abstract Map<String, AttributeValue> oldImage();

        @JsonProperty(APPROXIMATE_CREATION_DATE_TIME)
        public abstract Instant approximateCreationDateTime();
    }


    private abstract static class AttributeValueMixIn {
        // We don't need these annotations anymore since we're using a custom serializer
        // The AttributeValueSerializer will handle all the serialization logic
    }
}
