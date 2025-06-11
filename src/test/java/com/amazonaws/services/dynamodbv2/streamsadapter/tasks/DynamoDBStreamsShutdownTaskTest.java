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
package com.amazonaws.services.dynamodbv2.streamsadapter.tasks;

import com.amazonaws.services.dynamodbv2.streamsadapter.DynamoDBStreamsShardDetector;
import com.amazonaws.services.dynamodbv2.streamsadapter.DynamoDBStreamsShardSyncer;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.kinesis.checkpoint.ShardRecordProcessorCheckpointer;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.leases.UpdateField;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.lifecycle.ShutdownReason;
import software.amazon.kinesis.lifecycle.TaskResult;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class DynamoDBStreamsShutdownTaskTest {
    private static final String STREAM_ARN = "arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/2024-02-03T00:00:00.000";
    private static final String SHARD_ID = "shardId-000000000001";
    private static final String WORKER_ID = "worker-1";
    private static final long BACKOFF_TIME = 50L;

    @Mock
    private ShardRecordProcessor shardRecordProcessor;
    @Mock
    private ShardRecordProcessorCheckpointer recordProcessorCheckpointer;
    @Mock
    private Checkpointer checkpointer;
    @Mock
    private DynamoDBStreamsShardDetector shardDetector;
    @Mock
    private LeaseCoordinator leaseCoordinator;
    @Mock
    private LeaseRefresher leaseRefresher;
    @Mock
    private RecordsPublisher recordsPublisher;
    @Mock
    private DynamoDBStreamsShardSyncer shardSyncer;
    @Mock
    private LeaseCleanupManager leaseCleanupManager;

    private MetricsFactory metricsFactory;
    private ShardInfo shardInfo;
    private StreamIdentifier streamIdentifier;
    private DynamoDBStreamsShutdownTask task;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        metricsFactory = new NullMetricsFactory();
        streamIdentifier = StreamIdentifier.singleStreamInstance(KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(STREAM_ARN, false));
        shardInfo = new ShardInfo(SHARD_ID, null, null, ExtendedSequenceNumber.TRIM_HORIZON);

        when(leaseCoordinator.leaseRefresher()).thenReturn(leaseRefresher);
        when(recordProcessorCheckpointer.checkpointer()).thenReturn(checkpointer);
    }

    @Test
    void testShutdownWithLeaseLost() {
        task = createShutdownTask(ShutdownReason.LEASE_LOST, Collections.emptyList());

        task.call();

        verify(shardRecordProcessor).leaseLost(any(LeaseLostInput.class));
        verify(recordsPublisher).shutdown();
        verifyNoMoreInteractions(leaseRefresher);
    }

    @Test
    void testShutdownWithShardEndAndNoChildShards() throws Exception {
        task = createShutdownTask(ShutdownReason.SHARD_END, null);

        Lease currentLease = new Lease();
        currentLease.leaseKey(SHARD_ID);
        currentLease.leaseOwner(WORKER_ID);
        currentLease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);

        when(leaseCoordinator.getCurrentlyHeldLease(SHARD_ID)).thenReturn(currentLease);
        when(leaseRefresher.getLease(SHARD_ID)).thenReturn(currentLease);
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);

        task.call();

        verify(shardRecordProcessor).shardEnded(any(ShardEndedInput.class));
        verify(recordsPublisher).shutdown();
        verify(leaseRefresher, never()).updateLeaseWithMetaInfo(any(), any());
    }

    @Test
    void testShutdownWithShardEndAndChildShards() throws Exception {
        ChildShard child1 = ChildShard.builder().shardId("child-1").build();
        ChildShard child2 = ChildShard.builder().shardId("child-2").build();
        task = createShutdownTask(ShutdownReason.SHARD_END, Arrays.asList(child1, child2));

        Lease currentLease = new Lease();
        currentLease.leaseKey(SHARD_ID);
        currentLease.leaseOwner(WORKER_ID);
        currentLease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);

        when(leaseCoordinator.getCurrentlyHeldLease(SHARD_ID)).thenReturn(currentLease);
        when(leaseRefresher.getLease(SHARD_ID)).thenReturn(currentLease);
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        when(shardSyncer.createLeaseForChildShard(any(), any())).thenReturn(new Lease());

        task.call();

        verify(shardRecordProcessor).shardEnded(any(ShardEndedInput.class));
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor).shardEnded(any(ShardEndedInput.class));
        verify(leaseRefresher).updateLeaseWithMetaInfo(any(), eq(UpdateField.CHILD_SHARDS));
        verify(leaseRefresher, times(2)).createLeaseIfNotExists(any());
    }

    @Test
    public final void testCallWhenCreatingNewLeasesThrows() throws Exception {
        ChildShard child1 = ChildShard.builder().shardId("child-1").build();
        ChildShard child2 = ChildShard.builder().shardId("child-2").build();

        Lease currentLease = new Lease();
        currentLease.leaseKey(SHARD_ID);
        currentLease.leaseOwner(WORKER_ID);
        currentLease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);

        when(leaseCoordinator.getCurrentlyHeldLease(SHARD_ID)).thenReturn(currentLease);
        doThrow(new InvalidStateException("InvalidStateException is thrown"))
                .when(shardSyncer).createLeaseForChildShard(any(ChildShard.class), any(StreamIdentifier.class));
        when(leaseRefresher.getLease(SHARD_ID)).thenReturn(currentLease);
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        task = createShutdownTask(ShutdownReason.SHARD_END, Arrays.asList(child1, child2));
        final TaskResult result = task.call();
        verify(recordsPublisher).shutdown();
        verify(shardRecordProcessor, never()).shardEnded(any(ShardEndedInput.class));
        verify(shardRecordProcessor).leaseLost(LeaseLostInput.builder().build());
        verify(leaseCoordinator).dropLease(any(Lease.class));
    }

    @Test
    void testShutdownWithShardEndAndLostLease() throws Exception {
        task = createShutdownTask(ShutdownReason.SHARD_END, Collections.emptyList());

        when(leaseCoordinator.getCurrentlyHeldLease(SHARD_ID)).thenReturn(null);
        TaskResult result = task.call();

        verify(recordsPublisher).shutdown();
        verify(leaseCoordinator, never()).dropLease(any(Lease.class));
        verify(shardRecordProcessor).leaseLost(any(LeaseLostInput.class));
    }

    @Test
    void testShutdownWithShardEndAndMissingCheckpoint() throws Exception {
        task = createShutdownTask(ShutdownReason.SHARD_END, Collections.emptyList());

        Lease currentLease = new Lease();
        currentLease.leaseKey(SHARD_ID);
        currentLease.leaseOwner(WORKER_ID);
        currentLease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);

        when(leaseCoordinator.getCurrentlyHeldLease(SHARD_ID)).thenReturn(currentLease);
        when(leaseRefresher.getLease(SHARD_ID)).thenReturn(currentLease);
        when(recordProcessorCheckpointer.lastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("123"));

        TaskResult result = task.call();

        assertTrue(result.getException().getCause() instanceof IllegalArgumentException);
        assertTrue(result.getException().getCause().getMessage().contains("Application must checkpoint upon shard end"));

        verify(shardRecordProcessor).shardEnded(any(ShardEndedInput.class));
    }

    @Test
    void testShutdownWithShardEndAndLeaseNotHeld() throws Exception {
        ChildShard child1 = ChildShard.builder().shardId("child-1").build();
        task = createShutdownTask(ShutdownReason.SHARD_END, Collections.singletonList(child1));

        // We don't have the lease in currently held leases
        when(leaseCoordinator.getCurrentlyHeldLease(SHARD_ID)).thenReturn(null);

        task.call();

        // Verify that:
        // 1. We never try to get the lease from DDB
        verify(leaseRefresher, never()).getLease(any());
        // 2. We never try to create child leases
        verify(leaseRefresher, never()).createLeaseIfNotExists(any());
        // 3. We never try to update the lease
        verify(leaseRefresher, never()).updateLeaseWithMetaInfo(any(), any());
        // 4. We call leaseLost on the processor
        verify(shardRecordProcessor).leaseLost(any(LeaseLostInput.class));
        // 5. We still shutdown the publisher
        verify(recordsPublisher).shutdown();
        // 6. We never try to checkpoint
        verify(recordProcessorCheckpointer, never()).sequenceNumberAtShardEnd(any());
        verify(recordProcessorCheckpointer, never()).largestPermittedCheckpointValue(any());
    }

    private DynamoDBStreamsShutdownTask createShutdownTask(ShutdownReason reason, List<ChildShard> childShards) {
        return new DynamoDBStreamsShutdownTask(
                shardInfo,
                shardDetector,
                shardRecordProcessor,
                recordProcessorCheckpointer,
                reason,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                true,
                false,
                leaseCoordinator,
                BACKOFF_TIME,
                recordsPublisher,
                shardSyncer,
                metricsFactory,
                childShards,
                streamIdentifier,
                leaseCleanupManager
        );
    }
}