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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Custom deserializer for AttributeValue to handle JSON structure correctly.
 */
public class AttributeValueDeserializer extends JsonDeserializer<AttributeValue> {
    @Override
    public AttributeValue deserialize(JsonParser p, DeserializationContext ctxt) throws IOException,
            JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        return parseAttributeValue(node);
    }

    private AttributeValue parseAttributeValue(JsonNode node) throws IOException {
        AttributeValue.Builder builder = AttributeValue.builder();
        if (node.has("S")) {
            return builder.s(node.get("S").asText()).build();
        } else if (node.has("N")) {
            return builder.n(node.get("N").asText()).build();
        } else if (node.has("B")) {
            byte[] binaryData = node.get("B").binaryValue();
            return builder.b(SdkBytes.fromByteArray(binaryData)).build();
        } else if (node.has("BOOL")) {
            return builder.bool(node.get("BOOL").asBoolean()).build();
        } else if (node.has("NULL")) {
            return builder.nul(node.get("NULL").asBoolean()).build();
        } else if (node.has("SS")) {
            JsonNode ssNode = node.get("SS");
            List<String> ss = new ArrayList<>();
            for (JsonNode item : ssNode) {
                ss.add(item.asText());
            }
            return builder.ss(ss).build();
        } else if (node.has("NS")) {
            JsonNode nsNode = node.get("NS");
            List<String> ns = new ArrayList<>();
            for (JsonNode item : nsNode) {
                ns.add(item.asText());
            }
            return builder.ns(ns).build();
        } else if (node.has("BS")) {
            JsonNode bsNode = node.get("BS");
            List<SdkBytes> bs = new ArrayList<>();
            for (JsonNode item : bsNode) {
                bs.add(SdkBytes.fromByteArray(item.binaryValue()));
            }
            return builder.bs(bs).build();
        } else if (node.has("M")) {
            JsonNode mapNode = node.get("M");
            Map<String, AttributeValue> map = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = mapNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                map.put(field.getKey(), parseAttributeValue(field.getValue()));
            }
            return builder.m(map).build();
        } else if (node.has("L")) {
            JsonNode listNode = node.get("L");
            List<AttributeValue> list = new ArrayList<>();
            for (JsonNode item : listNode) {
                list.add(parseAttributeValue(item));
            }
            return builder.l(list).build();
        }
        // Default case - empty AttributeValue
        return builder.build();
    }
}
