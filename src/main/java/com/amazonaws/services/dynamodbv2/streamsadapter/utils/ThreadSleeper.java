/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Simple abstraction over Thread.sleep() to allow unit testing of backoff mechanisms.
 */
public class ThreadSleeper implements Sleeper {
    private static final Log LOG = LogFactory.getLog(ThreadSleeper.class);

    @Override public void sleep(long intervalInMillis) {
        try {
            Thread.sleep(intervalInMillis);
        } catch (InterruptedException ie) {
            LOG.debug("ThreadSleeper sleep  was interrupted ", ie);
            Thread.currentThread().interrupt();
        }
    }
}
