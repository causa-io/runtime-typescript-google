import { JsonObjectSerializer, ObjectSerializer } from '@causa/runtime';
import { NestJsModuleOverrider } from '@causa/runtime/nestjs/testing';
import { Message, PubSub, Subscription, Topic } from '@google-cloud/pubsub';
import { setTimeout } from 'timers/promises';
import * as uuid from 'uuid';
import { getConfigurationKeyForTopic } from '../configuration.js';
import { PUBSUB_PUBLISHER_CONFIGURATION_GETTER_INJECTION_NAME } from '../publisher.module.js';

/**
 * The default duration (in milliseconds) after which `expectMessageInTopic` times out.
 */
const DEFAULT_EXPECT_TIMEOUT = 2000;

/**
 * When the `expect` operation for a message fails, the wait duration (in milliseconds) before trying again.
 */
const DURATION_BETWEEN_EXPECT_ATTEMPTS = 50;

/**
 * A received Pub/Sub message that was deserialized.
 */
export type ReceivedPubSubEvent = {
  /**
   * The attributes of the Pub/Sub message.
   */
  attributes: Record<string, string>;

  /**
   * The ordering key of the Pub/Sub message.
   */
  orderingKey: string | undefined;

  /**
   * The parsed event.
   */
  event: any;
};

/**
 * Options for the {@link PubSubFixture.expectMessageInTopic} method.
 */
type ExpectMessageInTopicOptions = {
  /**
   * The maximum time (in milliseconds) to wait for a message before giving up.
   * Defaults to `2000`.
   */
  timeout?: number;
};

/**
 * A utility class managing temporary Pub/Sub topics and listening to messages published to them.
 */
export class PubSubFixture {
  /**
   * The Pub/Sub client to use.
   */
  readonly pubSub: PubSub;

  /**
   * The (de)serializer to use for Pub/Sub messages.
   */
  readonly serializer: ObjectSerializer;

  /**
   * The dictionary of monitored temporary topics.
   * The key is the name of the event topic (not broker-specific).
   */
  readonly fixtures: Record<
    string,
    {
      /**
       * The created temporary Pub/Sub topic.
       */
      topic: Topic;

      /**
       * The subscription appending messages to the array.
       */
      subscription: Subscription;

      /**
       * The array of received messages from the subscription.
       */
      messages: ReceivedPubSubEvent[];
    }
  > = {};

  /**
   * Creates a new {@link PubSubFixture}.
   *
   * @param options Options for the fixture.
   */
  constructor(
    options: {
      /**
       * The Pub/Sub client to use.
       */
      pubSub?: PubSub;

      /**
       * The (de)serializer to use for Pub/Sub messages.
       */
      serializer?: ObjectSerializer;
    } = {},
  ) {
    this.pubSub = options.pubSub ?? new PubSub();
    this.serializer = options.serializer ?? new JsonObjectSerializer();
  }

  /**
   * Creates a new temporary topic and starts listening to messages published to it.
   *
   * @param sourceTopicName The original name of the topic, i.e. the one used in production.
   * @param eventType The type of the event published to the topic, used for deserialization.
   * @returns The configuration key and value for the created temporary topic.
   */
  async create(
    sourceTopicName: string,
    eventType: { new (): any },
  ): Promise<Record<string, string>> {
    await this.delete(sourceTopicName);

    const suffix = uuid.v4().slice(-10);
    const topicName = `${sourceTopicName}-${suffix}`;
    const subscriptionName = `fixture-${suffix}`;

    // This ensures the project ID is populated in the Pub/Sub client.
    // Because the configuration is cached, it should be okay to call this multiple times.
    await this.pubSub.getClientConfig();

    const [topic] = await this.pubSub.createTopic(topicName);
    const [subscription] = await topic.createSubscription(subscriptionName);

    this.fixtures[sourceTopicName] = { topic, subscription, messages: [] };

    subscription.on('message', async (message: Message) => {
      const fixture = this.fixtures[sourceTopicName];
      if (!fixture || fixture.topic !== topic) {
        return;
      }

      const event = await this.serializer.deserialize(eventType, message.data);
      fixture.messages.push({
        attributes: message.attributes,
        orderingKey: message.orderingKey ? message.orderingKey : undefined,
        event,
      });

      message.ack();
    });

    return { [getConfigurationKeyForTopic(sourceTopicName)]: topic.name };
  }

  /**
   * Creates several temporary topics and returns the configuration (environment variables) for the
   *
   * @param topicsAndTypes A dictionary of topics and their corresponding event types.
   * @returns The configuration for Pub/Sub topics, with the IDs of the created topics.
   */
  async createMany(
    topicsAndTypes: Record<string, { new (): any }>,
  ): Promise<Record<string, string>> {
    const configurations = await Promise.all(
      Object.entries(topicsAndTypes).map(async ([topicName, eventType]) =>
        this.create(topicName, eventType),
      ),
    );

    return Object.assign({}, ...configurations);
  }

  /**
   * Uses {@link PubSubFixture.createMany} to create temporary topics and returns a {@link NestJsModuleOverrider} to
   * override the Pub/Sub publisher configuration.
   *
   * @param topicsAndTypes A dictionary of topics and their corresponding event types.
   * @returns The {@link NestJsModuleOverrider} to use to override the Pub/Sub publisher configuration.
   */
  async createWithOverrider(
    topicsAndTypes: Record<string, { new (): any }>,
  ): Promise<NestJsModuleOverrider> {
    const configuration = await this.createMany(topicsAndTypes);
    return (builder) =>
      builder
        .overrideProvider(PUBSUB_PUBLISHER_CONFIGURATION_GETTER_INJECTION_NAME)
        .useValue((key: string) => configuration[key]);
  }

  /**
   * Checks that the given message has been published to the specified topic.
   *
   * @param sourceTopicName The original name of the event topic.
   * @param expectedMessage The message expected to have been published.
   *   This can be an `expect` expression, e.g. `expect.objectContaining({})`.
   * @param options Options for the expectation.
   */
  async expectMessageInTopic(
    sourceTopicName: string,
    expectedMessage: any,
    options: ExpectMessageInTopicOptions = {},
  ): Promise<void> {
    const fixture = this.fixtures[sourceTopicName];
    if (!fixture) {
      throw new Error(`Fixture for topic '${sourceTopicName}' does not exist.`);
    }

    const timeoutTime =
      new Date().getTime() + (options.timeout ?? DEFAULT_EXPECT_TIMEOUT);

    while (true) {
      try {
        expect(fixture.messages).toContainEqual(expectedMessage);
        return;
      } catch (e) {
        if (new Date().getTime() >= timeoutTime) {
          throw e;
        }

        await setTimeout(DURATION_BETWEEN_EXPECT_ATTEMPTS);
      }
    }
  }

  /**
   * Uses {@link PubSubFixture.expectMessageInTopic} to check that the given event has been published to the specified
   * topic. The `expectedEvent` is the payload of the message, i.e. the `event` property.
   *
   * @param sourceTopicName The original name of the event topic.
   * @param expectedEvent The event expected to have been published.
   * @param options Options for the expectation.
   */
  async expectEventInTopic(
    sourceTopicName: string,
    expectedEvent: any,
    options: ExpectMessageInTopicOptions = {},
  ): Promise<void> {
    await this.expectMessageInTopic(
      sourceTopicName,
      expect.objectContaining({ event: expectedEvent }),
      options,
    );
  }

  /**
   * Checks that no message has been published to the given topic.
   *
   * @param sourceTopicName The original name of the topic, i.e. the one used in production.
   */
  async expectNoMessageInTopic(sourceTopicName: string): Promise<void> {
    const fixture = this.fixtures[sourceTopicName];
    if (!fixture) {
      throw new Error(`Fixture for topic '${sourceTopicName}' does not exist.`);
    }

    const numMessages = fixture.messages.length;
    if (numMessages > 0) {
      throw new Error(
        `Expected 0 messages in '${sourceTopicName}' but found ${numMessages}.`,
      );
    }
  }

  /**
   * Clears all the messages received from the temporary topics.
   */
  clear() {
    Object.values(this.fixtures).forEach((f) => {
      f.messages = [];
    });
  }

  /**
   * Deletes the temporary topic for the corresponding "production" topic.
   *
   * @param sourceTopicName The original name of the topic, i.e. the one used in production.
   */
  async delete(sourceTopicName: string): Promise<void> {
    const fixture = this.fixtures[sourceTopicName];
    if (!fixture) {
      return;
    }

    delete this.fixtures[sourceTopicName];

    fixture.subscription.removeAllListeners();
    await fixture.subscription.delete();

    await fixture.topic.delete();
  }

  /**
   * Deletes all previously created temporary topics.
   */
  async deleteAll(): Promise<void> {
    await Promise.all(
      Object.keys(this.fixtures).map((sourceTopicName) =>
        this.delete(sourceTopicName),
      ),
    );
  }
}
