package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;

public class GetShardIteratorRequestAdapterTest {
    private final String TEST_STRING = "TestString";

    @Mock
    private GetShardIteratorRequest mockRequest;

    private GetShardIteratorRequestAdapter adapter;

    @Before
    public void setUpTest() {
        MockitoAnnotations.initMocks(this);
        adapter = new GetShardIteratorRequestAdapter(mockRequest);
    }

    @Test
    public void testGetStreamId() {
        when(mockRequest.getStreamName()).thenReturn(TEST_STRING);
        String actual = adapter.getStreamId();
        assertEquals(TEST_STRING, actual);
    }

    @Test
    public void testSetStreamId() {
        adapter.setStreamId(TEST_STRING);
        verify(mockRequest, times(1)).setStreamName(TEST_STRING);
    }

    @Test
    public void testWithStreamId() {
        Object actual = adapter.withStreamId(TEST_STRING);
        assertEquals(adapter, actual);
    }

    @Test
    public void testGetShardId() {
        when(mockRequest.getShardId()).thenReturn(TEST_STRING);
        String actual = adapter.getShardId();
        assertEquals(TEST_STRING, actual);
    }

    @Test
    public void testSetShardId() {
        adapter.setShardId(TEST_STRING);
        verify(mockRequest, times(1)).setShardId(TEST_STRING);
    }

    @Test
    public void testWithShardId() {
        Object actual = adapter.withShardId(TEST_STRING);
        assertEquals(adapter, actual);
    }

    @Test
    public void testGetSequenceNumber() {
        when(mockRequest.getStartingSequenceNumber()).thenReturn(TEST_STRING);
        String actual = adapter.getSequenceNumber();
        assertEquals(TEST_STRING, actual);
    }

    @Test
    public void testSetSequenceNumber() {
        adapter.setSequenceNumber(TEST_STRING);
        verify(mockRequest, times(1)).setStartingSequenceNumber(TEST_STRING);
    }

    @Test
    public void testWithSequenceNumber() {
        Object actual = adapter.withSequenceNumber(TEST_STRING);
        assertEquals(adapter, actual);
    }

    @Test
    public void testGetShardIteratorType() {
        when(mockRequest.getShardIteratorType()).thenReturn(TEST_STRING);
        String actual = adapter.getShardIteratorType();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetShardIteratorTypeFailure() {
        adapter.setShardIteratorType(TEST_STRING);
    }

    @Test
    public void testSetShardIteratorTypeAsType() {
        adapter.setShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST);
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testWithShardIteratorTypeFailure() {
        adapter.withShardIteratorType(TEST_STRING);
    }

    @Test
    public void testWithShardIteratorTypeAsType() {
        Object actual = adapter.withShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST);
        assertEquals(adapter, actual);
    }

    @Test
    public void testRealData() {
        GetShardIteratorRequest request = createRequest();
        GetShardIteratorRequestAdapter requestAdapter = new GetShardIteratorRequestAdapter(request);
        assertEquals(request.getStartingSequenceNumber(), requestAdapter.getSequenceNumber());
        assertEquals(request.getShardId(), requestAdapter.getShardId());
        assertEquals(request.getShardIteratorType(), requestAdapter.getShardIteratorType());
        assertEquals(request.getStreamName(), requestAdapter.getStreamId());
    }

    private GetShardIteratorRequest createRequest() {
        return new GetShardIteratorRequest()
            .withShardId(TEST_STRING)
            .withStartingSequenceNumber(TEST_STRING)
            .withShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST)
            .withStreamName(TEST_STRING);
    }

}
