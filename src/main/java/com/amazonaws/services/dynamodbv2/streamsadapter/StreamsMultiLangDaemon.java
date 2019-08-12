/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.multilang.MultiLangDaemon;
import com.amazonaws.services.kinesis.multilang.MultiLangDaemonConfig;

/**
 * Main app that launches the worker that runs the multi-language record processor.
 * Requires a properties file containing configuration for this daemon and the KCL.
 * <p>
 * This version extends the KCL's MultiLangDaemon to use DynamoDB Streams instead of
 * Kinesis.
 */
public class StreamsMultiLangDaemon {

    private static final Log LOG = LogFactory.getLog(StreamsMultiLangDaemon.class);

    /**
     * @param args Accepts a single argument, that argument is a properties file which provides KCL configuration as
     *             well as the name of an executable.
     */
    public static void main(String[] args) {

        if (args.length == 0) {
            MultiLangDaemon.printUsage(System.err, "You must provide a properties file");
            System.exit(1);
        }
        MultiLangDaemonConfig config = null;
        try {
            config = new MultiLangDaemonConfig(args[0]);
        } catch (IOException | IllegalArgumentException e) {
            MultiLangDaemon.printUsage(System.err, "You must provide a valid properties file");
            System.exit(1);
        }

        final ExecutorService executorService = config.getExecutorService();
        final Worker worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(config.getRecordProcessorFactory(), config.getKinesisClientLibConfiguration(), executorService);

        // Daemon
        final MultiLangDaemon daemon = new MultiLangDaemon(worker);

        final Future<Integer> future = executorService.submit(daemon);
        try {
            System.exit(future.get());
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Encountered an error while running daemon", e);
        }
        System.exit(1);
    }

}
