import {
  type EventAttributes,
  type EventPublisher,
  JsonObjectSerializer,
  type ObjectSerializer,
  type PreparedEvent,
  type PublishOptions,
} from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import {
  PubSub,
  Topic,
  type PublishOptions as TopicPublishOptions,
} from '@google-cloud/pubsub';
import type { OnApplicationShutdown } from '@nestjs/common';
import { getConfigurationKeyForTopic } from './configuration.js';
import { PubSubTopicNotConfiguredError } from './errors.js';

/**
 * The default options to use when publishing messages.
 * Batching is disabled as the most common use case is to publish a single message at a time, for which latency is more
 * important than throughput.
 */
const DEFAULT_PUBLISH_OPTIONS: TopicPublishOptions = {
  batching: { maxMessages: 1 },
};

/**
 * Options for the {@link PubSubPublisher}.
 */
export type PubSubPublisherOptions = {
  /**
   * The Pub/Sub client to use.
   */
  pubSub?: PubSub;

  /**
   * The serializer to use to convert events to buffers.
   * Defaults to {@link JsonObjectSerializer}.
   */
  serializer?: ObjectSerializer;

  /**
   * A function to get the configuration value for a given key.
   * Used to retrieve the Pub/Sub topic ID for a given event topic.
   * Defaults to reading the values from the environment.
   */
  configurationGetter?: (key: string) => string | undefined;

  /**
   * The default options to use when publishing messages.
   * This is used to instantiate the Pub/Sub {@link Topic}s.
   */
  publishOptions?: TopicPublishOptions;

  /**
   * The options to use when publishing messages.
   * Keys are event topics, values are {@link TopicPublishOptions}.
   * This inherits and overrides the {@link PubSubPublisherOptions.publishOptions} for a given topic.
   */
  topicPublishOptions?: Record<string, TopicPublishOptions>;
};

/**
 * An implementation of the {@link EventPublisher} using Google Pub/Sub as the broker.
 */
export class PubSubPublisher implements EventPublisher, OnApplicationShutdown {
  /**
   * The {@link PubSub} client to use.
   */
  readonly pubSub: PubSub;

  /**
   * The serializer to use to convert events to buffers.
   */
  readonly serializer: ObjectSerializer;

  /**
   * A function to get the configuration value for a given key.
   * Used to retrieve the Pub/Sub topic ID for a given event topic.
   */
  private readonly getConfiguration: (key: string) => string | undefined;

  /**
   * A cache of Pub/Sub {@link Topic}s to which messages can be published.
   */
  private readonly topicCache: Record<string, Topic> = {};

  /**
   * The options to use when publishing messages.
   * This is used to instantiate the Pub/Sub {@link Topic}s.
   */
  readonly publishOptions: TopicPublishOptions | undefined;

  /**
   * The options to use when publishing messages.
   * Keys are event topics, values are {@link TopicPublishOptions}.
   * This inherits and overrides the {@link PubSubPublisher.publishOptions} for a given topic.
   */
  readonly topicPublishOptions: Record<string, TopicPublishOptions>;

  /**
   * Creates a new {@link PubSubPublisher}.
   *
   * @param logger The logger to use.
   * @param options Options for the publisher.
   */
  constructor(
    private readonly logger: Logger,
    options: PubSubPublisherOptions = {},
  ) {
    this.logger.setContext(PubSubPublisher.name);
    this.pubSub = options.pubSub ?? new PubSub();
    this.serializer = options.serializer ?? new JsonObjectSerializer();
    this.getConfiguration =
      options.configurationGetter ?? ((key) => process.env[key]);
    this.publishOptions = options.publishOptions ?? DEFAULT_PUBLISH_OPTIONS;
    this.topicPublishOptions = options.topicPublishOptions ?? {};
  }

  /**
   * Returns the Pub/Sub {@link Topic} to which messages can be published for a given event topic.
   * If the topic has not been used before, it will be created using the {@link PubSubPublisher.publishOptions}.
   *
   * @param topicName The name of the event topic.
   * @returns The Pub/Sub {@link Topic} to which messages can be published.
   */
  private getTopic(topicName: string): Topic {
    const cachedTopic = this.topicCache[topicName];
    if (cachedTopic) {
      return cachedTopic;
    }

    const key = getConfigurationKeyForTopic(topicName);
    const topicId = this.getConfiguration(key);
    if (!topicId) {
      throw new PubSubTopicNotConfiguredError(topicName);
    }

    const options = {
      ...this.publishOptions,
      ...this.topicPublishOptions[topicName],
    };
    const topic = this.pubSub.topic(topicId, options);

    this.topicCache[topicName] = topic;
    return topic;
  }

  async prepare(
    topic: string,
    event: object,
    options: PublishOptions = {},
  ): Promise<PreparedEvent> {
    const data = await this.serializer.serialize(event);

    const defaultAttributes: EventAttributes = {};
    if ('id' in event && typeof event.id === 'string') {
      defaultAttributes.eventId = event.id;
    }
    if ('producedAt' in event && event.producedAt instanceof Date) {
      defaultAttributes.producedAt = event.producedAt.toISOString();
    }
    if ('name' in event && typeof event.name === 'string') {
      defaultAttributes.eventName = event.name;
    }

    const attributes = {
      ...defaultAttributes,
      ...options?.attributes,
    };
    const key = options?.key;

    return { topic, data, attributes, key };
  }

  async publish(
    topicOrPreparedEvent: string | PreparedEvent,
    event?: object,
    options: PublishOptions = {},
  ): Promise<void> {
    const isPrepared = typeof topicOrPreparedEvent !== 'string';

    const { topic, data, attributes, key } = isPrepared
      ? topicOrPreparedEvent
      : await this.prepare(topicOrPreparedEvent, event!, options);

    const pubSubTopic = this.getTopic(topic);

    const messageInfo: Record<string, string> = {
      topic,
      pubSubTopic: pubSubTopic.name,
    };
    if (attributes && 'eventId' in attributes) {
      messageInfo.eventId = attributes.eventId;
    }

    const messageId = await pubSubTopic.publishMessage({
      data,
      attributes,
      orderingKey: key,
    });

    this.logger.info(
      { publishedMessage: { ...messageInfo, messageId } },
      'Published message to Pub/Sub.',
    );
  }

  async flush(): Promise<void> {
    await Promise.all(
      Object.values(this.topicCache).map((topic) => topic.flush()),
    );
  }

  async onApplicationShutdown(): Promise<void> {
    await this.flush();
  }
}
