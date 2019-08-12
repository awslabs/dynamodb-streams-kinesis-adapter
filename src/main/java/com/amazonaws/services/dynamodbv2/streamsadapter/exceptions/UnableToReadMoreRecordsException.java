/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
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
