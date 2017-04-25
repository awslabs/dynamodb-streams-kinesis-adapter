/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.dynamodbv2.streamsadapter.functionals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.TestRecordProcessorFactory;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.TestUtil;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;

/**
 * This base class sets up DynamoDB, Kinesis Adapter and DynamoDB streams clients used by a KCL worker operating on DynamoDB
 * Streams. It also creates required DynamoDB tables.
 */
public abstract class FunctionalTestBase {
    private static final Log LOG = LogFactory.getLog(FunctionalTestBase.class);

    protected AmazonDynamoDBStreams streamsClient;
    protected AmazonDynamoDBStreamsAdapterClient adapterClient;
    protected AmazonDynamoDB dynamoDBClient;

    protected AWSCredentialsProvider credentials;
    protected String streamId;

    protected Worker worker;
    protected TestRecordProcessorFactory recordProcessorFactory;
    protected ExecutorService workerThread;

    private static String accessKeyId = "KCLIntegTest";
    private static String secretAccessKey = "dummy";

    protected static String srcTable = "kcl-integ-test-src";
    protected static String destTable = "kcl-integ-test-dest";
    protected static String leaseTable = "kcl-integ-test-leases";

    protected static int THREAD_SLEEP_5S = 5000;
    protected static int THREAD_SLEEP_2S = 2000;
    protected static String KCL_WORKER_ID = "kcl-integration-test-worker";

    @Before
    public void setup() {
        credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey));

        dynamoDBClient = new AmazonDynamoDBClient(credentials);
        dynamoDBClient.setEndpoint("http://localhost:4567");
        streamsClient = new AmazonDynamoDBStreamsClient(credentials);
        streamsClient.setEndpoint("http://localhost:4567");

        adapterClient = new AmazonDynamoDBStreamsAdapterClient(streamsClient);

        streamId = TestUtil.createTable(dynamoDBClient, srcTable, true /*With streams enabled*/);
        TestUtil.createTable(dynamoDBClient, destTable, false /* No streams */);

        TestUtil.waitForTableActive(dynamoDBClient, srcTable);
        TestUtil.waitForTableActive(dynamoDBClient, destTable);
    }

    @After
    public void teardown() {
        dynamoDBClient.deleteTable(new DeleteTableRequest().withTableName(srcTable));
        dynamoDBClient.deleteTable(new DeleteTableRequest().withTableName(destTable));
        dynamoDBClient.deleteTable(new DeleteTableRequest().withTableName(leaseTable));
    }

    protected void startKCLWorker(KinesisClientLibConfiguration workerConfig) {

        recordProcessorFactory = new TestRecordProcessorFactory(dynamoDBClient, destTable);

        LOG.info("Creating worker for stream: " + streamId);
        worker = StreamsWorkerFactory
            .createDynamoDbStreamsWorker(recordProcessorFactory, workerConfig, adapterClient, dynamoDBClient, new NullMetricsFactory(), Executors.newCachedThreadPool());

        LOG.info("Starting worker...");
        workerThread = Executors.newSingleThreadExecutor();
        workerThread.submit(worker);

        workerThread.shutdown(); //This will wait till the KCL worker exits
    }

    protected void shutDownKCLWorker() throws Exception {
        worker.shutdown();

        if (!workerThread.awaitTermination(THREAD_SLEEP_5S, TimeUnit.MILLISECONDS)) {
            workerThread.shutdownNow();
        }

        LOG.info("Processing complete.");
    }

}
