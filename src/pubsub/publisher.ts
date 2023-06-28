import {
  Event,
  EventAttributes,
  EventPublisher,
  JsonObjectSerializer,
  ObjectSerializer,
  PublishOptions,
  getDefaultLogger,
} from '@causa/runtime';
import {
  PubSub,
  Topic,
  PublishOptions as TopicPublishOptions,
} from '@google-cloud/pubsub';
import { Logger } from 'pino';
import { getConfigurationKeyForTopic } from './configuration.js';
import { PubSubTopicNotConfiguredError } from './errors.js';

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
   * The options to use when publishing messages.
   * This is used to instantiate the Pub/Sub {@link Topic}s.
   */
  publishOptions?: TopicPublishOptions;

  /**
   * The logger to use.
   * Defaults to {@link getDefaultLogger}.
   */
  logger?: Logger;
};

/**
 * An implementation of the {@link EventPublisher} using Google Pub/Sub as the broker.
 */
export class PubSubPublisher implements EventPublisher {
  /**
   * The {@link PubSub} client to use.
   */
  private readonly pubSub: PubSub;

  /**
   * The serializer to use to convert events to buffers.
   */
  private readonly serializer: ObjectSerializer;

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
   * The logger to use.
   */
  private readonly logger: Logger;

  /**
   * The options to use when publishing messages.
   * This is used to instantiate the Pub/Sub {@link Topic}s.
   */
  private readonly publishOptions: TopicPublishOptions | undefined;

  /**
   * Creates a new {@link PubSubPublisher}.
   *
   * @param options Options for the publisher.
   */
  constructor(options: PubSubPublisherOptions = {}) {
    this.pubSub = options.pubSub ?? new PubSub();
    this.serializer = options.serializer ?? new JsonObjectSerializer();
    this.getConfiguration =
      options.configurationGetter ?? ((key) => process.env[key]);
    this.publishOptions = options.publishOptions;
    this.logger = options.logger ?? getDefaultLogger();
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

    const topic = this.pubSub.topic(topicId, this.publishOptions);

    this.topicCache[topicName] = topic;
    return topic;
  }

  async publish(
    topic: string,
    event: Event,
    options: PublishOptions = {},
  ): Promise<void> {
    const data = await this.serializer.serialize(event);
    const pubSubTopic = this.getTopic(topic);
    const attributes: EventAttributes = {
      ...options.attributes,
      producedAt: event.producedAt.toISOString(),
      eventName: event.name,
      eventId: event.id,
    };
    const orderingKey = options.key;
    const baseLogData = {
      topic,
      eventId: event.id,
      pubSubTopic: pubSubTopic.name,
    };

    try {
      const pubSubMessageId = await pubSubTopic.publishMessage({
        data,
        attributes,
        orderingKey,
      });

      this.logger.info(
        { ...baseLogData, pubSubMessageId },
        'Published message to Pub/Sub.',
      );
    } catch (error: any) {
      this.logger.error(
        {
          ...baseLogData,
          pubSubMessage: data.toString('base64'),
          pubSubAttributes: attributes,
          errorMessage: error.message,
          errorStack: error.stack,
        },
        'Failed to publish message to Pub/Sub.',
      );
      throw error;
    }
  }

  async flush(): Promise<void> {
    await Promise.all(
      Object.values(this.topicCache).map((topic) => topic.flush()),
    );
  }
}
