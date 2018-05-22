/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
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
