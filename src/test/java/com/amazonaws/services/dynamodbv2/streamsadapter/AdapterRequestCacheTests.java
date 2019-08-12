/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.GetRecordsRequestAdapter;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;

public class AdapterRequestCacheTests {

    private static final int CACHE_SIZE = 50;

    @Test
    public void testSanityConstructor() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        assertTrue(requestCache instanceof AdapterRequestCache);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroCapacity() {
        new AdapterRequestCache(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeCapacity() {
        Random r = new Random();
        int positiveNumber = r.nextInt(Integer.MAX_VALUE - 1) + 1;
        new AdapterRequestCache(-1 * positiveNumber);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRequestAdd() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        requestCache.addEntry(null, new GetRecordsRequestAdapter(new GetRecordsRequest()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRequestAdapterAdd() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        requestCache.addEntry(new GetRecordsRequest(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRequestGet() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        requestCache.getEntry(null);
    }

    @Test
    public void testCacheSanity() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        GetRecordsRequest request = new GetRecordsRequest();
        GetRecordsRequestAdapter requestAdapter = new GetRecordsRequestAdapter(request);
        requestCache.addEntry(request, requestAdapter);
        AmazonWebServiceRequest entry = requestCache.getEntry(request);
        assertEquals(System.identityHashCode(requestAdapter), System.identityHashCode(entry));
    }

    @Test
    public void testEviction() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        int testLength = 2 * CACHE_SIZE;
        List<AmazonWebServiceRequest> requests = new ArrayList<AmazonWebServiceRequest>(testLength);
        List<AmazonWebServiceRequest> requestAdapters = new ArrayList<AmazonWebServiceRequest>(testLength);
        for (int i = 0; i < testLength; i++) {
            // Construct requests
            GetRecordsRequest request = new GetRecordsRequest();
            GetRecordsRequestAdapter requestAdapter = new GetRecordsRequestAdapter(request);
            // Store references to request for validation
            requests.add(request);
            requestAdapters.add(requestAdapter);
            // Add entry to the request cache
            requestCache.addEntry(request, requestAdapter);

            // Verify request cache
            for (int j = 0; j <= i; j++) {
                AmazonWebServiceRequest expected = requestAdapters.get(j);
                AmazonWebServiceRequest actual = requestCache.getEntry(requests.get(j));
                if (j <= i - CACHE_SIZE) {
                    assertNull(actual);
                } else {
                    assertEquals(System.identityHashCode(expected), System.identityHashCode(actual));
                }
            }

        }
    }

}
