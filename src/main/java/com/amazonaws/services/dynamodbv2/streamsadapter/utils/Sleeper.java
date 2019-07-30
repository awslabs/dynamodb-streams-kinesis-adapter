/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.utils;

/**
 * Interface to implement mechanisms to inject specified delay
 * in processing similar to Thread.sleep(long millis).
 */
public interface Sleeper {

    void sleep(long intervalInMillis);
}
