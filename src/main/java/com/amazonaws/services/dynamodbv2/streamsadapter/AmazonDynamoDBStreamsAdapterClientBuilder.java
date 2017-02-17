package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.client.AwsSyncClientParams;
import com.amazonaws.client.builder.AwsSyncClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;

/**
 * Builder for {@link AmazonDynamoDBStreamsAdapterClient} as a generic {@link AmazonKinesis} instance
 * Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 2/17/17.
 */
@NotThreadSafe
public class AmazonDynamoDBStreamsAdapterClientBuilder
		extends AwsSyncClientBuilder<AmazonDynamoDBStreamsAdapterClientBuilder, AmazonKinesis>
{
	private static final ClientConfigurationFactory CLIENT_CONFIG_FACTORY = new ClientConfigurationFactory();
	
	/**
	 * Private no-arg constructor
	 */
	private AmazonDynamoDBStreamsAdapterClientBuilder()
	{
		super( CLIENT_CONFIG_FACTORY );
	}
	
	/**
	 * @return a new instance of this builder with default settings
	 */
	public static AmazonDynamoDBStreamsAdapterClientBuilder standard()
	{
		return new AmazonDynamoDBStreamsAdapterClientBuilder();
	}
	
	/**
	 * Construct a {@link AmazonDynamoDBStreamsAdapterClient} instance with the given parameters
	 *
	 * @param params client configuration parameters
	 * @return a fully configured {@link AmazonDynamoDBStreamsAdapterClient} instance
	 */
	@Override
	protected AmazonKinesis build( AwsSyncClientParams params )
	{
		return new AmazonDynamoDBStreamsAdapterClient( params );
	}
}
