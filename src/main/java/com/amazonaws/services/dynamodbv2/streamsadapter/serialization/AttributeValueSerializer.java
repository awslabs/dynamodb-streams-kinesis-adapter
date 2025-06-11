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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.io.IOException;

/**
 * Custom serializer for AttributeValue to handle empty lists and maps correctly.
 */
public class AttributeValueSerializer extends JsonSerializer<AttributeValue> {
    @Override
    public void serialize(AttributeValue value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        if (value.s() != null) {
            gen.writeStringField("S", value.s());
        }
        if (value.n() != null) {
            gen.writeStringField("N", value.n());
        }
        if (value.b() != null) {
            gen.writeBinaryField("B", value.b().asByteArray());
        }
        if (value.bool() != null) {
            gen.writeBooleanField("BOOL", value.bool());
        }
        if (value.nul() != null) {
            gen.writeBooleanField("NULL", value.nul());
        }
        if (value.hasSs()) {
            gen.writeFieldName("SS");
            gen.writeStartArray();
            for (String s : value.ss()) {
                gen.writeString(s);
            }
            gen.writeEndArray();
        }
        if (value.hasNs()) {
            gen.writeFieldName("NS");
            gen.writeStartArray();
            for (String n : value.ns()) {
                gen.writeString(n);
            }
            gen.writeEndArray();
        }
        if (value.hasBs()) {
            gen.writeFieldName("BS");
            gen.writeStartArray();
            for (SdkBytes b : value.bs()) {
                gen.writeBinary(b.asByteArray());
            }
            gen.writeEndArray();
        }
        if (value.hasM()) {
            gen.writeFieldName("M");
            if (value.m().isEmpty()) {
                gen.writeStartObject();
                gen.writeEndObject();
            } else {
                gen.writeObject(value.m());
            }
        }
        if (value.hasL()) {
            gen.writeFieldName("L");
            if (value.l().isEmpty()) {
                gen.writeStartArray();
                gen.writeEndArray();
            } else {
                gen.writeObject(value.l());
            }
        }
        gen.writeEndObject();
    }
}
