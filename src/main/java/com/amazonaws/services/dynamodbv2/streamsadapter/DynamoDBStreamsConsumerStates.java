package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.BlockOnParentShardTask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitializeTask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ProcessTask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownNotificationTask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.TaskType;


/**
 * Top level container for all the possible states a {@link DynamoDBStreamsShardConsumer} can be in. The logic for creation of tasks,
 * and state transitions is contained within the {@link ConsumerState} objects.
 *
 * <h2>State Diagram</h2>
 *
 * <pre>
 *       +-------------------+
 *       | Waiting on Parent |                               +------------------+
 *  +----+       Shard       |                               |     Shutdown     |
 *  |    |                   |          +--------------------+   Notification   |
 *  |    +----------+--------+          |  Shutdown:         |     Requested    |
 *  |               | Success           |   Requested        +-+-------+--------+
 *  |               |                   |                      |       |
 *  |        +------+-------------+     |                      |       | Shutdown:
 *  |        |    Initializing    +-----+                      |       |  Requested
 *  |        |                    |     |                      |       |
 *  |        |                    +-----+-------+              |       |
 *  |        +---------+----------+     |       | Shutdown:    | +-----+-------------+
 *  |                  | Success        |       |  Terminated  | |     Shutdown      |
 *  |                  |                |       |  Zombie      | |   Notification    +-------------+
 *  |           +------+-------------+  |       |              | |     Complete      |             |
 *  |           |     Processing     +--+       |              | ++-----------+------+             |
 *  |       +---+                    |          |              |  |           |                    |
 *  |       |   |                    +----------+              |  |           | Shutdown:          |
 *  |       |   +------+-------------+          |              \  /           |  Requested         |
 *  |       |          |                        |               \/            +--------------------+
 *  |       |          |                        |               ||
 *  |       | Success  |                        |               || Shutdown:
 *  |       +----------+                        |               ||  Terminated
 *  |                                           |               ||  Zombie
 *  |                                           |               ||
 *  |                                           |               ||
 *  |                                           |           +---++--------------+
 *  |                                           |           |   Shutting Down   |
 *  |                                           +-----------+                   |
 *  |                                                       |                   |
 *  |                                                       +--------+----------+
 *  |                                                                |
 *  |                                                                | Shutdown:
 *  |                                                                |  All Reasons
 *  |                                                                |
 *  |                                                                |
 *  |      Shutdown:                                        +--------+----------+
 *  |        All Reasons                                    |     Shutdown      |
 *  +-------------------------------------------------------+     Complete      |
 *                                                          |                   |
 *                                                          +-------------------+
 * </pre>
 */
public class DynamoDBStreamsConsumerStates {

    enum ShardConsumerState {
        // @formatter:off
        WAITING_ON_PARENT_SHARDS(new BlockedOnParentState()),
        INITIALIZING(new InitializingState()),
        PROCESSING(new ProcessingState()),
        SHUTDOWN_REQUESTED(new ShutdownNotificationState()),
        SHUTTING_DOWN(new ShuttingDownState()),
        SHUTDOWN_COMPLETE(new ShutdownCompleteState());
        //@formatter:on

        private final ConsumerState consumerState;

        ShardConsumerState(ConsumerState consumerState) {
            this.consumerState = consumerState;
        }

        public ConsumerState getConsumerState() {
            return consumerState;
        }
    }


    /**
     * Represents a the current state of the consumer. This handles the creation of tasks for the consumer, and what to
     * do when a transition occurs.
     *
     */
    interface ConsumerState {
        /**
         * Creates a new task for this state using the passed in consumer to build the task. If there is no task
         * required for this state it may return a null value. {@link ConsumerState}'s are allowed to modify the
         * consumer during the execution of this method.
         *
         * @param consumer
         *            the consumer to use build the task, or execute state.
         * @return a valid task for this state or null if there is no task required.
         */
        ITask createTask(DynamoDBStreamsShardConsumer consumer);

        /**
         * Provides the next state of the consumer upon success of the task return by
         * {@link ConsumerState#createTask(DynamoDBStreamsShardConsumer consumer)}.
         *
         * @return the next state that the consumer should transition to, this may be the same object as the current
         *         state.
         */
        ConsumerState successTransition();

        /**
         * Provides the next state of the consumer when a shutdown has been requested. The returned state is dependent
         * on the current state, and the shutdown reason.
         *
         * @param shutdownReason
         *            the reason that a shutdown was requested
         * @return the next state that the consumer should transition to, this may be the same object as the current
         *         state.
         */
        ConsumerState shutdownTransition(ShutdownReason shutdownReason);

        /**
         * The type of task that {@link ConsumerState#createTask(DynamoDBStreamsShardConsumer)} would return. This is always a valid state
         * even if createTask would return a null value.
         *
         * @return the type of task that this state represents.
         */
        TaskType getTaskType();

        /**
         * An enumeration represent the type of this state. Different consumer states may return the same
         * {@link ShardConsumerState}.
         *
         * @return the type of consumer state this represents.
         */
        ShardConsumerState getState();

        boolean isTerminal();
    }

    /**
     * The initial state that any {@link DynamoDBStreamsShardConsumer} should start in.
     */
    static final ConsumerState INITIAL_STATE = ShardConsumerState.WAITING_ON_PARENT_SHARDS.getConsumerState();

    /**
     * This is the initial state of a shard consumer. This causes the consumer to remain blocked until the all parent
     * shards have been completed.
     *
     * <h2>Valid Transitions</h2>
     * <dl>
     * <dt>Success</dt>
     * <dd>Transition to the initializing state to allow the record processor to be initialized in preparation of
     * processing.</dd>
     * <dt>Shutdown</dt>
     * <dd>
     * <dl>
     * <dt>All Reasons</dt>
     * <dd>Transitions to {@link ShutdownCompleteState}. Since the record processor was never initialized it can't be
     * informed of the shutdown.</dd>
     * </dl>
     * </dd>
     * </dl>
     */
    static class BlockedOnParentState implements ConsumerState {

        @Override
        public ITask createTask(DynamoDBStreamsShardConsumer consumer) {
            return new BlockOnParentShardTask(consumer.getShardInfo(), consumer.getLeaseManager(),
                    consumer.getParentShardPollIntervalMillis());
        }

        @Override
        public ConsumerState successTransition() {
            return ShardConsumerState.INITIALIZING.getConsumerState();
        }

        @Override
        public ConsumerState shutdownTransition(ShutdownReason shutdownReason) {
            return ShardConsumerState.SHUTTING_DOWN.getConsumerState();
        }

        @Override
        public TaskType getTaskType() {
            return TaskType.BLOCK_ON_PARENT_SHARDS;
        }

        @Override
        public ShardConsumerState getState() {
            return ShardConsumerState.WAITING_ON_PARENT_SHARDS;
        }

        @Override
        public boolean isTerminal() {
            return false;
        }
    }

    /**
     * This state is responsible for initializing the record processor with the shard information.
     * <h2>Valid Transitions</h2>
     * <dl>
     * <dt>Success</dt>
     * <dd>Transitions to the processing state which will begin to send records to the record processor</dd>
     * <dt>Shutdown</dt>
     * <dd>At this point the record processor has been initialized, but hasn't processed any records. This requires that
     * the record processor be notified of the shutdown, even though there is almost no actions the record processor
     * could take.
     * <dl>
     * <dt>{@link ShutdownReason#REQUESTED}</dt>
     * <dd>Transitions to the {@link ShutdownNotificationState}</dd>
     * <dt>{@link ShutdownReason#ZOMBIE}</dt>
     * <dd>Transitions to the {@link ShuttingDownState}</dd>
     * <dt>{@link ShutdownReason#TERMINATE}</dt>
     * <dd>
     * <p>
     * This reason should not occur, since terminate is triggered after reaching the end of a shard. Initialize never
     * makes an requests to Kinesis for records, so it can't reach the end of a shard.
     * </p>
     * <p>
     * Transitions to the {@link ShuttingDownState}
     * </p>
     * </dd>
     * </dl>
     * </dd>
     * </dl>
     */
    static class InitializingState implements ConsumerState {

        @Override
        public ITask createTask(DynamoDBStreamsShardConsumer consumer) {
            return new InitializeTask(consumer.getShardInfo(),
                    consumer.getRecordProcessor(),
                    consumer.getCheckpoint(),
                    consumer.getRecordProcessorCheckpointer(),
                    consumer.getDataFetcher(),
                    consumer.getTaskBackoffTimeMillis(),
                    consumer.getStreamConfig(),
                    consumer.getGetRecordsCache());
        }

        @Override
        public ConsumerState successTransition() {
            return ShardConsumerState.PROCESSING.getConsumerState();
        }

        @Override
        public ConsumerState shutdownTransition(ShutdownReason shutdownReason) {
            return kinesisToDDBStateTransform(shutdownReason);
        }

        @Override
        public TaskType getTaskType() {
            return TaskType.INITIALIZE;
        }

        @Override
        public ShardConsumerState getState() {
            return ShardConsumerState.INITIALIZING;
        }

        @Override
        public boolean isTerminal() {
            return false;
        }
    }

    /**
     * This state is responsible for retrieving records from Kinesis, and dispatching them to the record processor.
     * While in this state the only way a transition will occur is if a shutdown has been triggered.
     * <h2>Valid Transitions</h2>
     * <dl>
     * <dt>Success</dt>
     * <dd>Doesn't actually transition, but instead returns the same state</dd>
     * <dt>Shutdown</dt>
     * <dd>At this point records are being retrieved, and processed. It's now possible for the consumer to reach the end
     * of the shard triggering a {@link ShutdownReason#TERMINATE}.
     * <dl>
     * <dt>{@link ShutdownReason#REQUESTED}</dt>
     * <dd>Transitions to the {@link ShutdownNotificationState}</dd>
     * <dt>{@link ShutdownReason#ZOMBIE}</dt>
     * <dd>Transitions to the {@link ShuttingDownState}</dd>
     * <dt>{@link ShutdownReason#TERMINATE}</dt>
     * <dd>Transitions to the {@link ShuttingDownState}</dd>
     * </dl>
     * </dd>
     * </dl>
     */
    static class ProcessingState implements ConsumerState {

        @Override
        public ITask createTask(DynamoDBStreamsShardConsumer consumer) {
            return new ProcessTask(consumer.getShardInfo(),
                    consumer.getStreamConfig(),
                    consumer.getRecordProcessor(),
                    consumer.getRecordProcessorCheckpointer(),
                    consumer.getDataFetcher(),
                    consumer.getTaskBackoffTimeMillis(),
                    consumer.isSkipShardSyncAtWorkerInitializationIfLeasesExist(),
                    consumer.getGetRecordsCache());
        }

        @Override
        public ConsumerState successTransition() {
            return ShardConsumerState.PROCESSING.getConsumerState();
        }

        @Override
        public ConsumerState shutdownTransition(ShutdownReason shutdownReason) {
            return kinesisToDDBStateTransform(shutdownReason);
        }

        @Override
        public TaskType getTaskType() {
            return TaskType.PROCESS;
        }

        @Override
        public ShardConsumerState getState() {
            return ShardConsumerState.PROCESSING;
        }

        @Override
        public boolean isTerminal() {
            return false;
        }
    }

    static final ConsumerState SHUTDOWN_REQUEST_COMPLETION_STATE = new ShutdownNotificationCompletionState();

    /**
     * This state occurs when a shutdown has been explicitly requested. This shutdown allows the record processor a
     * chance to checkpoint and prepare to be shutdown via the normal method. This state can only be reached by a
     * shutdown on the {@link InitializingState} or {@link ProcessingState}.
     *
     * <h2>Valid Transitions</h2>
     * <dl>
     * <dt>Success</dt>
     * <dd>Success shouldn't normally be called since the {@link DynamoDBStreamsShardConsumer} is marked for shutdown.</dd>
     * <dt>Shutdown</dt>
     * <dd>At this point records are being retrieved, and processed. An explicit shutdown will allow the record
     * processor one last chance to checkpoint, and then the {@link DynamoDBStreamsShardConsumer} will be held in an idle state.
     * <dl>
     * <dt>{@link ShutdownReason#REQUESTED}</dt>
     * <dd>Remains in the {@link ShardConsumerState#SHUTDOWN_REQUESTED}, but the state implementation changes to
     * {@link ShutdownNotificationCompletionState}</dd>
     * <dt>{@link ShutdownReason#ZOMBIE}</dt>
     * <dd>Transitions to the {@link ShuttingDownState}</dd>
     * <dt>{@link ShutdownReason#TERMINATE}</dt>
     * <dd>Transitions to the {@link ShuttingDownState}</dd>
     * </dl>
     * </dd>
     * </dl>
     */
    static class ShutdownNotificationState implements ConsumerState {

        @Override
        public ITask createTask(DynamoDBStreamsShardConsumer consumer) {
            return new ShutdownNotificationTask(consumer.getRecordProcessor(),
                    consumer.getRecordProcessorCheckpointer(),
                    consumer.getShutdownNotification(),
                    consumer.getShardInfo());
        }

        @Override
        public ConsumerState successTransition() {
            return SHUTDOWN_REQUEST_COMPLETION_STATE;
        }

        @Override
        public ConsumerState shutdownTransition(ShutdownReason shutdownReason) {
            if (shutdownReason == ShutdownReason.REQUESTED) {
                return SHUTDOWN_REQUEST_COMPLETION_STATE;
            }
            return kinesisToDDBStateTransform(shutdownReason);
        }

        @Override
        public TaskType getTaskType() {
            return TaskType.SHUTDOWN_NOTIFICATION;
        }

        @Override
        public ShardConsumerState getState() {
            return ShardConsumerState.SHUTDOWN_REQUESTED;
        }

        @Override
        public boolean isTerminal() {
            return false;
        }
    }

    /**
     * Once the {@link ShutdownNotificationState} has been completed the {@link DynamoDBStreamsShardConsumer} must not re-enter any of the
     * processing states. This state idles the {@link DynamoDBStreamsShardConsumer} until the worker triggers the final shutdown state.
     *
     * <h2>Valid Transitions</h2>
     * <dl>
     * <dt>Success</dt>
     * <dd>
     * <p>
     * Success shouldn't normally be called since the {@link DynamoDBStreamsShardConsumer} is marked for shutdown.
     * </p>
     * <p>
     * Remains in the {@link ShutdownNotificationCompletionState}
     * </p>
     * </dd>
     * <dt>Shutdown</dt>
     * <dd>At this point the {@link DynamoDBStreamsShardConsumer} has notified the record processor of the impending shutdown, and is
     * waiting that notification. While waiting for the notification no further processing should occur on the
     * {@link DynamoDBStreamsShardConsumer}.
     * <dl>
     * <dt>{@link ShutdownReason#REQUESTED}</dt>
     * <dd>Remains in the {@link ShardConsumerState#SHUTDOWN_REQUESTED}, and the state implementation remains
     * {@link ShutdownNotificationCompletionState}</dd>
     * <dt>{@link ShutdownReason#ZOMBIE}</dt>
     * <dd>Transitions to the {@link ShuttingDownState}</dd>
     * <dt>{@link ShutdownReason#TERMINATE}</dt>
     * <dd>Transitions to the {@link ShuttingDownState}</dd>
     * </dl>
     * </dd>
     * </dl>
     */
    static class ShutdownNotificationCompletionState implements ConsumerState {

        @Override
        public ITask createTask(DynamoDBStreamsShardConsumer consumer) {
            return null;
        }

        @Override
        public ConsumerState successTransition() {
            return this;
        }

        @Override
        public ConsumerState shutdownTransition(ShutdownReason shutdownReason) {
            if (shutdownReason != ShutdownReason.REQUESTED) {
                return kinesisToDDBStateTransform(shutdownReason);
            }
            return this;
        }

        @Override
        public TaskType getTaskType() {
            return TaskType.SHUTDOWN_NOTIFICATION;
        }

        @Override
        public ShardConsumerState getState() {
            return ShardConsumerState.SHUTDOWN_REQUESTED;
        }

        @Override
        public boolean isTerminal() {
            return false;
        }
    }

    /**
     * This state is entered if the {@link DynamoDBStreamsShardConsumer} loses its lease, or reaches the end of the shard.
     *
     * <h2>Valid Transitions</h2>
     * <dl>
     * <dt>Success</dt>
     * <dd>
     * <p>
     * Success shouldn't normally be called since the {@link DynamoDBStreamsConsumerStates} is marked for shutdown.
     * </p>
     * <p>
     * Transitions to the {@link ShutdownCompleteState}
     * </p>
     * </dd>
     * <dt>Shutdown</dt>
     * <dd>At this point the record processor has processed the final shutdown indication, and depending on the shutdown
     * reason taken the correct course of action. From this point on there should be no more interactions with the
     * record processor or {@link DynamoDBStreamsShardConsumer}.
     * <dl>
     * <dt>{@link ShutdownReason#REQUESTED}</dt>
     * <dd>
     * <p>
     * This should not occur as all other {@link ShutdownReason}s take priority over it.
     * </p>
     * <p>
     * Transitions to {@link ShutdownCompleteState}
     * </p>
     * </dd>
     * <dt>{@link ShutdownReason#ZOMBIE}</dt>
     * <dd>Transitions to the {@link ShutdownCompleteState}</dd>
     * <dt>{@link ShutdownReason#TERMINATE}</dt>
     * <dd>Transitions to the {@link ShutdownCompleteState}</dd>
     * </dl>
     * </dd>
     * </dl>
     */
    static class ShuttingDownState implements ConsumerState {

        @Override
        public ITask createTask(DynamoDBStreamsShardConsumer consumer) {
            return new DynamoDBStreamsShutdownTask(consumer.getShardInfo(),
                    consumer.getRecordProcessor(),
                    consumer.getRecordProcessorCheckpointer(),
                    consumer.getShutdownReason(),
                    consumer.getStreamConfig().getStreamProxy(),
                    consumer.getStreamConfig().getInitialPositionInStream(),
                    consumer.isCleanupLeasesOfCompletedShards(),
                    consumer.isIgnoreUnexpectedChildShards(),
                    consumer.getLeaseCoordinator(),
                    consumer.getTaskBackoffTimeMillis(),
                    consumer.getGetRecordsCache(), consumer.getShardSyncer(),
                    consumer.getShardSyncStrategy());
        }

        @Override
        public ConsumerState successTransition() {
            return ShardConsumerState.SHUTDOWN_COMPLETE.getConsumerState();
        }

        @Override
        public ConsumerState shutdownTransition(ShutdownReason shutdownReason) {
            return ShardConsumerState.SHUTDOWN_COMPLETE.getConsumerState();
        }

        @Override
        public TaskType getTaskType() {
            return TaskType.SHUTDOWN;
        }

        @Override
        public ShardConsumerState getState() {
            return ShardConsumerState.SHUTTING_DOWN;
        }

        @Override
        public boolean isTerminal() {
            return false;
        }
    }

    /**
     * This is the final state for the {@link DynamoDBStreamsConsumerStates}. This occurs once all shutdown activities are completed.
     *
     * <h2>Valid Transitions</h2>
     * <dl>
     * <dt>Success</dt>
     * <dd>
     * <p>
     * Success shouldn't normally be called since the {@link DynamoDBStreamsConsumerStates} is marked for shutdown.
     * </p>
     * <p>
     * Remains in the {@link ShutdownCompleteState}
     * </p>
     * </dd>
     * <dt>Shutdown</dt>
     * <dd>At this point the all shutdown activites are completed, and the {@link DynamoDBStreamsConsumerStates} should not take any
     * further actions.
     * <dl>
     * <dt>{@link ShutdownReason#REQUESTED}</dt>
     * <dd>
     * <p>
     * This should not occur as all other {@link ShutdownReason}s take priority over it.
     * </p>
     * <p>
     * Remains in {@link ShutdownCompleteState}
     * </p>
     * </dd>
     * <dt>{@link ShutdownReason#ZOMBIE}</dt>
     * <dd>Remains in {@link ShutdownCompleteState}</dd>
     * <dt>{@link ShutdownReason#TERMINATE}</dt>
     * <dd>Remains in {@link ShutdownCompleteState}</dd>
     * </dl>
     * </dd>
     * </dl>
     */
    static class ShutdownCompleteState implements ConsumerState {

        @Override
        public ITask createTask(DynamoDBStreamsShardConsumer consumer) {
            if (consumer.getShutdownNotification() != null) {
                consumer.getShutdownNotification().shutdownComplete();
            }
            return null;
        }

        @Override
        public ConsumerState successTransition() {
            return this;
        }

        @Override
        public ConsumerState shutdownTransition(ShutdownReason shutdownReason) {
            return this;
        }

        @Override
        public TaskType getTaskType() {
            return TaskType.SHUTDOWN_COMPLETE;
        }

        @Override
        public ShardConsumerState getState() {
            return ShardConsumerState.SHUTDOWN_COMPLETE;
        }

        @Override
        public boolean isTerminal() {
            return true;
        }
    }

    public static ConsumerState kinesisToDDBStateTransform(ShutdownReason reason){

        switch(reason){

            case REQUESTED:
                return ShardConsumerState.SHUTDOWN_REQUESTED.getConsumerState();

            case TERMINATE:
            case ZOMBIE:
                return ShardConsumerState.SHUTTING_DOWN.getConsumerState();

            default:
                throw new IllegalArgumentException("Unknown reason: " + reason); //if incorrect state is inputted
        }
    }
}
