/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.exceptions;

/**
 * This exception is thrown when records have been trimmed and the user has specified to not continue processing.
 */
public class UnableToReadMoreRecordsException extends RuntimeException {

    private static final long serialVersionUID = 7280447889113524297L;

    public UnableToReadMoreRecordsException(String message, Throwable cause) {
        super(message, cause);
    }

}
