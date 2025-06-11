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
