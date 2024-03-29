package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.DataFetcherResult;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.GetRecordsRetrievalStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SynchronousGetRecordsRetrievalStrategy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBStreamsDataFetcherTest {

    @Mock
    private IKinesisProxy kinesisProxy;

    private static final int MAX_RECORDS = 1;
    private static final String SHARD_ID = "shardId-1";
    private static final String AT_SEQUENCE_NUMBER = ShardIteratorType.AT_SEQUENCE_NUMBER.toString();
    private static final ShardInfo SHARD_INFO = new ShardInfo(SHARD_ID, null, null, null);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final InitialPositionInStreamExtended INITIAL_POSITION_AT_TIMESTAMP =
            InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(1000));

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        MetricsHelper.startScope(new NullMetricsFactory(), "DynamoDBStreamsDataFetcherTest");
    }

    /**
     * Test initialize() with the LATEST iterator instruction
     */
    @Test
    public final void testInitializeLatest() throws Exception {
        testInitializeAndFetch(ShardIteratorType.LATEST.toString(),
                ShardIteratorType.LATEST.toString(),
                INITIAL_POSITION_LATEST);
    }

    /**
     * Test initialize() with the TIME_ZERO iterator instruction
     */
    @Test
    public final void testInitializeTimeZero() throws Exception {
        testInitializeAndFetch(ShardIteratorType.TRIM_HORIZON.toString(),
                ShardIteratorType.TRIM_HORIZON.toString(),
                INITIAL_POSITION_TRIM_HORIZON);
    }


    /**
     * Test initialize() when a flushpoint exists.
     */
    @Test
    public final void testInitializeFlushpoint() throws Exception {
        testInitializeAndFetch("foo", "123", INITIAL_POSITION_LATEST);
    }

    /**
     * Test initialize() with an invalid iterator instruction
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testInitializeInvalid() throws Exception {
        testInitializeAndFetch("foo", null, INITIAL_POSITION_LATEST);
    }

    @Test
    public void testadvanceIteratorTo() throws Exception {
        IKinesisProxy kinesis = mock(IKinesisProxy.class);
        ICheckpoint checkpoint = mock(ICheckpoint.class);

        DynamoDBStreamsDataFetcher fetcher = new DynamoDBStreamsDataFetcher(kinesis, SHARD_INFO);
        GetRecordsRetrievalStrategy getRecordsRetrievalStrategy = new SynchronousGetRecordsRetrievalStrategy(fetcher);

        String iteratorA = "foo";
        String iteratorB = "bar";
        String seqA = "123";
        String seqB = "456";
        GetRecordsResult outputA = new GetRecordsResult();
        List<Record> recordsA = new ArrayList<Record>();
        outputA.setRecords(recordsA);
        outputA.setNextShardIterator("nextShardIteratorA");
        GetRecordsResult outputB = new GetRecordsResult();
        List<Record> recordsB = new ArrayList<Record>();
        outputB.setRecords(recordsB);
        outputB.setNextShardIterator("nextShardIteratorB");

        when(kinesis.getIterator(SHARD_ID, AT_SEQUENCE_NUMBER, seqA)).thenReturn(iteratorA);
        when(kinesis.getIterator(SHARD_ID, AT_SEQUENCE_NUMBER, seqB)).thenReturn(iteratorB);
        when(kinesis.get(iteratorA, MAX_RECORDS)).thenReturn(outputA);
        when(kinesis.get(iteratorB, MAX_RECORDS)).thenReturn(outputB);

        when(checkpoint.getCheckpoint(SHARD_ID)).thenReturn(new ExtendedSequenceNumber(seqA));
        fetcher.initialize(seqA, null);

        fetcher.advanceIteratorTo(seqA, null);
        Assert.assertEquals(recordsA, getRecordsRetrievalStrategy.getRecords(MAX_RECORDS).getRecords());

        fetcher.advanceIteratorTo(seqB, null);
        Assert.assertEquals(recordsB, getRecordsRetrievalStrategy.getRecords(MAX_RECORDS).getRecords());
    }

    @Test
    public void testadvanceIteratorToTrimHorizonAndLatest() throws Exception{
        IKinesisProxy kinesis = mock(IKinesisProxy.class);

        DynamoDBStreamsDataFetcher fetcher = new DynamoDBStreamsDataFetcher(kinesis, SHARD_INFO);

        String iteratorHorizon = "horizon";
        when(kinesis.getIterator(SHARD_ID, ShardIteratorType.TRIM_HORIZON.toString())).thenReturn(iteratorHorizon);
        fetcher.advanceIteratorTo(ShardIteratorType.TRIM_HORIZON.toString(), INITIAL_POSITION_TRIM_HORIZON);
        Assert.assertEquals(iteratorHorizon, fetcher.getNextIterator());

        String iteratorLatest = "latest";
        when(kinesis.getIterator(SHARD_ID, ShardIteratorType.LATEST.toString())).thenReturn(iteratorLatest);
        fetcher.advanceIteratorTo(ShardIteratorType.LATEST.toString(), INITIAL_POSITION_LATEST);
        Assert.assertEquals(iteratorLatest, fetcher.getNextIterator());
    }

    @Test
    public void testGetRecordsWithResourceNotFoundException() throws Exception {
        // Set up arguments used by proxy
        String nextIterator = "TestShardIterator";
        int maxRecords = 100;

        // Set up proxy mock methods
        IKinesisProxy mockProxy = mock(IKinesisProxy.class);
        doReturn(nextIterator).when(mockProxy).getIterator(SHARD_ID, ShardIteratorType.LATEST.toString());
        doThrow(new ResourceNotFoundException("Test Exception")).when(mockProxy).get(nextIterator, maxRecords);

        // Create data fectcher and initialize it with latest type checkpoint
        DynamoDBStreamsDataFetcher dataFetcher = new DynamoDBStreamsDataFetcher(mockProxy, SHARD_INFO);
        dataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);
        GetRecordsRetrievalStrategy getRecordsRetrievalStrategy = new SynchronousGetRecordsRetrievalStrategy(dataFetcher);
        // Call getRecords of dataFetcher which will throw an exception
        getRecordsRetrievalStrategy.getRecords(maxRecords);

        // Test shard has reached the end
        Assert.assertTrue("Shard should reach the end", dataFetcher.isShardEndReached());
    }

    @Test
    public void testNonNullGetRecords() throws Exception {
        String nextIterator = "TestIterator";
        int maxRecords = 100;

        IKinesisProxy mockProxy = mock(IKinesisProxy.class);
        when(mockProxy.getIterator(anyString(), anyString())).thenReturn("targetIterator");
        doThrow(new ResourceNotFoundException("Test Exception")).when(mockProxy).get(nextIterator, maxRecords);

        DynamoDBStreamsDataFetcher dataFetcher = new DynamoDBStreamsDataFetcher(mockProxy, SHARD_INFO);
        dataFetcher.initialize(SentinelCheckpoint.LATEST.toString(), INITIAL_POSITION_LATEST);

        DataFetcherResult dataFetcherResult = dataFetcher.getRecords(maxRecords);

        assertThat(dataFetcherResult, notNullValue());
    }

    @Test
    public void testFetcherDoesNotAdvanceWithoutAccept() {
        final String INITIAL_ITERATOR = "InitialIterator";
        final String NEXT_ITERATOR_ONE = "NextIteratorOne";
        final String NEXT_ITERATOR_TWO = "NextIteratorTwo";
        when(kinesisProxy.getIterator(anyString(), anyString())).thenReturn(INITIAL_ITERATOR);

        GetRecordsResult iteratorOneResults = new GetRecordsResult();
        iteratorOneResults.setNextShardIterator(NEXT_ITERATOR_ONE);
        when(kinesisProxy.get(eq(INITIAL_ITERATOR), anyInt())).thenReturn(iteratorOneResults);

        GetRecordsResult iteratorTwoResults = new GetRecordsResult();
        iteratorTwoResults.setNextShardIterator(NEXT_ITERATOR_TWO);
        when(kinesisProxy.get(eq(NEXT_ITERATOR_ONE), anyInt())).thenReturn(iteratorTwoResults);

        GetRecordsResult finalResult = new GetRecordsResult();
        finalResult.setNextShardIterator(null);
        when(kinesisProxy.get(eq(NEXT_ITERATOR_TWO), anyInt())).thenReturn(finalResult);

        DynamoDBStreamsDataFetcher dataFetcher = new DynamoDBStreamsDataFetcher(kinesisProxy, SHARD_INFO);
        dataFetcher.initialize("TRIM_HORIZON",
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));

        assertNoAdvance(dataFetcher, iteratorOneResults, INITIAL_ITERATOR);
        assertAdvanced(dataFetcher, iteratorOneResults, INITIAL_ITERATOR, NEXT_ITERATOR_ONE);

        assertNoAdvance(dataFetcher, iteratorTwoResults, NEXT_ITERATOR_ONE);
        assertAdvanced(dataFetcher, iteratorTwoResults, NEXT_ITERATOR_ONE, NEXT_ITERATOR_TWO);

        assertNoAdvance(dataFetcher, finalResult, NEXT_ITERATOR_TWO);
        assertAdvanced(dataFetcher, finalResult, NEXT_ITERATOR_TWO, null);

        verify(kinesisProxy, times(2)).get(eq(INITIAL_ITERATOR), anyInt());
        verify(kinesisProxy, times(2)).get(eq(NEXT_ITERATOR_ONE), anyInt());
        verify(kinesisProxy, times(2)).get(eq(NEXT_ITERATOR_TWO), anyInt());

        reset(kinesisProxy);

        DataFetcherResult terminal = dataFetcher.getRecords(100);
        assertThat(terminal.isShardEnd(), equalTo(true));
        assertThat(terminal.getResult(), notNullValue());
        GetRecordsResult terminalResult = terminal.getResult();
        assertThat(terminalResult.getRecords(), notNullValue());
        assertThat(terminalResult.getRecords(), empty());
        assertThat(terminalResult.getNextShardIterator(), nullValue());
        assertThat(terminal, equalTo(dataFetcher.TERMINAL_RESULT));

        verify(kinesisProxy, never()).get(anyString(), anyInt());
    }

    @Test
    public void testRestartIterator() throws Exception{
        GetRecordsResult getRecordsResult = mock(GetRecordsResult.class);
        GetRecordsResult restartGetRecordsResult = mock(GetRecordsResult.class);
        Record record = mock(Record.class);
        final String initialIterator = "InitialIterator";
        final String nextShardIterator = "NextShardIterator";
        final String restartShardIterator = "RestartIterator";
        final String restartNextShardIterator = "RestartNextIterator";
        final String sequenceNumber = "SequenceNumber";
        final String iteratorType = "AT_SEQUENCE_NUMBER";
        IKinesisProxy kinesisProxy = mock(IKinesisProxy.class);
        DynamoDBStreamsDataFetcher fetcher = new DynamoDBStreamsDataFetcher(kinesisProxy, SHARD_INFO);

        when(kinesisProxy.getIterator(eq(SHARD_ID), eq(InitialPositionInStream.LATEST.toString()))).thenReturn(initialIterator);
        when(kinesisProxy.get(eq(initialIterator), eq(10))).thenReturn(getRecordsResult);
        when(getRecordsResult.getRecords()).thenReturn(Collections.singletonList(record));
        when(getRecordsResult.getNextShardIterator()).thenReturn(nextShardIterator);
        when(record.getSequenceNumber()).thenReturn(sequenceNumber);

        fetcher.initialize(InitialPositionInStream.LATEST.toString(), INITIAL_POSITION_LATEST);
        verify(kinesisProxy).getIterator(eq(SHARD_ID), eq(InitialPositionInStream.LATEST.toString()));
        Assert.assertEquals(getRecordsResult, fetcher.getRecords(10).accept());
        verify(kinesisProxy).get(eq(initialIterator), eq(10));

        when(kinesisProxy.getIterator(eq(SHARD_ID), eq(iteratorType), eq(sequenceNumber))).thenReturn(restartShardIterator);
        when(restartGetRecordsResult.getNextShardIterator()).thenReturn(restartNextShardIterator);
        when(kinesisProxy.get(eq(restartShardIterator), eq(10))).thenReturn(restartGetRecordsResult);

        fetcher.restartIterator();
        Assert.assertEquals(restartGetRecordsResult, fetcher.getRecords(10).accept());
        verify(kinesisProxy).getIterator(eq(SHARD_ID), eq(iteratorType), eq(sequenceNumber));
        verify(kinesisProxy).get(eq(restartShardIterator), eq(10));
    }

    @Test (expected = IllegalStateException.class)
    public void testRestartIteratorNotInitialized() throws Exception {
        DynamoDBStreamsDataFetcher dataFetcher = new DynamoDBStreamsDataFetcher(kinesisProxy, SHARD_INFO);
        dataFetcher.restartIterator();
    }

    private DataFetcherResult assertAdvanced(DynamoDBStreamsDataFetcher dataFetcher, GetRecordsResult expectedResult,
                                             String previousValue, String nextValue) {
        DataFetcherResult acceptResult = dataFetcher.getRecords(100);
        assertThat(acceptResult.getResult(), equalTo(expectedResult));

        assertThat(dataFetcher.getNextIterator(), equalTo(previousValue));
        assertThat(dataFetcher.isShardEndReached(), equalTo(false));

        assertThat(acceptResult.accept(), equalTo(expectedResult));
        assertThat(dataFetcher.getNextIterator(), equalTo(nextValue));
        if (nextValue == null) {
            assertThat(dataFetcher.isShardEndReached(), equalTo(true));
        }

        verify(kinesisProxy, times(2)).get(eq(previousValue), anyInt());

        return acceptResult;
    }

    private DataFetcherResult assertNoAdvance(DynamoDBStreamsDataFetcher dataFetcher, GetRecordsResult expectedResult,
                                              String previousValue) {
        assertThat(dataFetcher.getNextIterator(), equalTo(previousValue));
        DataFetcherResult noAcceptResult = dataFetcher.getRecords(100);
        assertThat(noAcceptResult.getResult(), equalTo(expectedResult));

        assertThat(dataFetcher.getNextIterator(), equalTo(previousValue));

        verify(kinesisProxy).get(eq(previousValue), anyInt());

        return noAcceptResult;
    }

    private void testInitializeAndFetch(String iteratorType,
                                        String seqNo,
                                        InitialPositionInStreamExtended initialPositionInStream) throws Exception {
        IKinesisProxy kinesis = mock(IKinesisProxy.class);
        String iterator = "foo";
        List<Record> expectedRecords = new ArrayList<Record>();
        GetRecordsResult response = new GetRecordsResult();
        response.setRecords(expectedRecords);
        response.setNextShardIterator("testNextShardIterator");

        when(kinesis.getIterator(SHARD_ID, initialPositionInStream.getTimestamp())).thenReturn(iterator);
        when(kinesis.getIterator(SHARD_ID, AT_SEQUENCE_NUMBER, seqNo)).thenReturn(iterator);
        when(kinesis.getIterator(SHARD_ID, iteratorType)).thenReturn(iterator);
        when(kinesis.get(iterator, MAX_RECORDS)).thenReturn(response);

        ICheckpoint checkpoint = mock(ICheckpoint.class);
        when(checkpoint.getCheckpoint(SHARD_ID)).thenReturn(new ExtendedSequenceNumber(seqNo));

        DynamoDBStreamsDataFetcher fetcher = new DynamoDBStreamsDataFetcher(kinesis, SHARD_INFO);
        GetRecordsRetrievalStrategy getRecordsRetrievalStrategy = new SynchronousGetRecordsRetrievalStrategy(fetcher);
        fetcher.initialize(seqNo, initialPositionInStream);
        List<Record> actualRecords = getRecordsRetrievalStrategy.getRecords(MAX_RECORDS).getRecords();

        Assert.assertEquals(expectedRecords, actualRecords);
    }

}
