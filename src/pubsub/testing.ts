import { JsonObjectSerializer, type ObjectSerializer } from '@causa/runtime';
import type {
  AppFixture,
  EventFixture,
  Fixture,
  NestJsModuleOverrider,
} from '@causa/runtime/nestjs/testing';
import { Message, PubSub, Subscription, Topic } from '@google-cloud/pubsub';
import { HttpStatus, type Type } from '@nestjs/common';
import 'jest-extended';
import { setTimeout } from 'timers/promises';
import * as uuid from 'uuid';
import { getConfigurationKeyForTopic } from './configuration.js';
import { PUBSUB_PUBLISHER_CONFIGURATION_GETTER_INJECTION_NAME } from './publisher.module.js';

/**
 * The default duration (in milliseconds) after which `expectMessageInTopic` times out.
 */
const DEFAULT_EXPECT_TIMEOUT = 2000;

/**
 * The default delay (in milliseconds) before checking that no message has been received.
 */
const DEFAULT_EXPECT_NO_MESSAGE_DELAY = 50;

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
 * Options for the {@link PubSubFixture.expectMessage} method.
 */
export type ExpectMessageOptions = {
  /**
   * The maximum time (in milliseconds) to wait for a message before giving up.
   * Defaults to `2000`.
   */
  timeout?: number;

  /**
   * When `true`, the received messages must exactly match the expected messages (same members), rather than merely
   * including all of them.
   * Defaults to `false`.
   */
  exact?: boolean;
};

/**
 * Options for the {@link PubSubFixture.expectEvent} method.
 */
export type ExpectEventOptions = ExpectMessageOptions & {
  /**
   * The attributes expected to have been published with the event.
   * This may contain only a subset of the attributes.
   */
  attributes?: Record<string, string>;
};

/**
 * Options when making a request to an endpoint handling Pub/Sub events using an {@link EventRequester}.
 */
export type EventRequesterOptions = {
  /**
   * The attributes to add to the Pub/Sub message.
   * Using `undefined` values allows removing the attributes set by default.
   */
  attributes?: Record<string, string | undefined>;

  /**
   * The expected status code when making the request.
   * Default is `200`.
   */
  expectedStatus?: number;

  /**
   * The time to set as the publication time of the Pub/Sub message.
   */
  publishTime?: Date;
};

/**
 * A function that makes a query to an endpoint handling Pub/Sub events and tests the response.
 */
export type EventRequester = (
  event: object,
  options?: EventRequesterOptions,
) => Promise<void>;

/**
 * A utility class managing temporary Pub/Sub topics and listening to messages published to them.
 */
export class PubSubFixture implements Fixture, EventFixture {
  /**
   * The parent {@link AppFixture}.
   */
  private appFixture!: AppFixture;

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
  readonly topics: Record<
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
   * @param topicsAndTypes The dictionary of topics to test and their event types.
   * @param options Options for the fixture.
   */
  constructor(
    private readonly topicsAndTypes: Record<string, Type>,
    options: {
      /**
       * The (de)serializer to use for Pub/Sub messages.
       */
      serializer?: ObjectSerializer;
    } = {},
  ) {
    this.pubSub = new PubSub();
    this.serializer = options.serializer ?? new JsonObjectSerializer();
  }

  /**
   * Creates a new temporary topic and starts listening to messages published to it.
   *
   * @param sourceTopic The original name of the topic, i.e. the one used in production.
   * @param eventType The type of the event published to the topic, used for deserialization.
   * @returns The configuration key and value for the created temporary topic.
   */
  private async create(
    sourceTopic: string,
    eventType: Type,
  ): Promise<Record<string, string>> {
    await this.deleteTopic(sourceTopic);

    const suffix = uuid.v4().slice(-10);
    const topicName = `${sourceTopic}-${suffix}`;
    const subscriptionName = `fixture-${suffix}`;

    // This ensures the project ID is populated in the Pub/Sub client.
    // Because the configuration is cached, it should be okay to call this multiple times.
    await this.pubSub.getClientConfig();

    const [topic] = await this.pubSub.createTopic(topicName);
    const [subscription] = await topic.createSubscription(subscriptionName);

    this.topics[sourceTopic] = { topic, subscription, messages: [] };

    subscription.on('message', async (message: Message) => {
      const topicFixture = this.topics[sourceTopic];
      if (topicFixture?.topic !== topic) {
        return;
      }

      const event = await this.serializer.deserialize(eventType, message.data);
      topicFixture.messages.push({
        attributes: message.attributes,
        orderingKey: message.orderingKey ? message.orderingKey : undefined,
        event,
      });

      message.ack();
    });

    return { [getConfigurationKeyForTopic(sourceTopic)]: topic.name };
  }

  async init(appFixture: AppFixture): Promise<NestJsModuleOverrider> {
    this.appFixture = appFixture;

    const configurations = await Promise.all(
      Object.entries(this.topicsAndTypes).map(([topic, eventType]) =>
        this.create(topic, eventType),
      ),
    );
    const configuration = Object.assign({}, ...configurations);

    return (builder) =>
      builder
        .overrideProvider(PUBSUB_PUBLISHER_CONFIGURATION_GETTER_INJECTION_NAME)
        .useValue((key: string) => configuration[key]);
  }

  /**
   * Creates an {@link EventRequester} for an endpoint handling Pub/Sub messages.
   * If the `event` passed to the {@link EventRequester} conforms to the `Event` interface (if it has `producedAt`,
   * `name` and / or `id` properties), the default attributes are set in the Pub/Sub message. Default attributes can be
   * overridden (or removed by passing `undefined`) using {@link EventRequesterOptions.attributes}.
   *
   * @param endpoint The endpoint to query.
   * @param options Options when creating the requester.
   * @returns The {@link EventRequester}.
   */
  makeRequester(
    endpoint: string,
    options: {
      /**
       * The default expected status code when making a request.
       */
      expectedStatus?: number;
    } = {},
  ): EventRequester {
    return async (event, requestOptions) => {
      const messageId = uuid.v4();
      const publishTime = (
        requestOptions?.publishTime ?? new Date()
      ).toISOString();
      const buffer = await this.serializer.serialize(event);
      const data = buffer.toString('base64');

      // Default attributes if the event conforms to the `Event` interface.
      const defaultAttributes: Record<string, string> = {};
      if ('producedAt' in event && event.producedAt instanceof Date) {
        defaultAttributes.producedAt = event.producedAt.toISOString();
      }
      if ('name' in event && typeof event.name === 'string') {
        defaultAttributes.eventName = event.name;
      }
      if ('id' in event && typeof event.id === 'string') {
        defaultAttributes.eventId = event.id;
      }
      const attributes = {
        ...defaultAttributes,
        ...requestOptions?.attributes,
      };

      const payload = {
        message: {
          messageId,
          message_id: messageId,
          publishTime,
          publish_time: publishTime,
          attributes,
          data,
        },
        subscription: 'subscription',
      };
      const expectedStatus =
        requestOptions?.expectedStatus ??
        options.expectedStatus ??
        HttpStatus.OK;

      await this.appFixture.request
        .post(endpoint)
        .send(payload)
        .expect(expectedStatus);
    };
  }

  /**
   * Gets the received messages for the specified topic.
   *
   * @param topic The original name of the event topic.
   * @returns The received messages.
   */
  getReceivedMessages(topic: string): ReceivedPubSubEvent[] {
    const fixture = this.topics[topic];
    if (!fixture) {
      throw new Error(`Fixture for topic '${topic}' does not exist.`);
    }

    return fixture.messages;
  }

  /**
   * Checks that the given messages have been published to the specified topic.
   * Each expected message must match a distinct received message.
   *
   * @param topic The original name of the event topic.
   * @param expectedMessages The messages expected to have been published.
   *   Each can be an `expect` expression, e.g. `expect.objectContaining({})`.
   * @param options Options for the expectation.
   */
  async expectMessages(
    topic: string,
    expectedMessages: Partial<ReceivedPubSubEvent>[],
    options: ExpectMessageOptions = {},
  ): Promise<void> {
    const timeoutTime =
      Date.now() + (options.timeout ?? DEFAULT_EXPECT_TIMEOUT);

    while (true) {
      const actualMessages = this.getReceivedMessages(topic);

      try {
        if (options.exact) {
          expect(actualMessages).toIncludeSameMembers(expectedMessages);
        } else {
          expect(actualMessages).toIncludeAllMembers(expectedMessages);
        }
        return;
      } catch (e) {
        if (Date.now() >= timeoutTime) {
          if (expectedMessages.length === 1 && actualMessages.length === 1) {
            // This throws with a clearer message because the single received message is actually compared to the
            // expected message.
            expect(actualMessages[0]).toEqual(expectedMessages[0]);
          }

          throw e;
        }

        await setTimeout(DURATION_BETWEEN_EXPECT_ATTEMPTS);
      }
    }
  }

  /**
   * Checks that the given message has been published to the specified topic.
   *
   * @param topic The original name of the event topic.
   * @param expectedMessage The message expected to have been published.
   *   This can be an `expect` expression, e.g. `expect.objectContaining({})`.
   * @param options Options for the expectation.
   */
  async expectMessage(
    topic: string,
    expectedMessage: any,
    options: ExpectMessageOptions = {},
  ): Promise<void> {
    await this.expectMessages(topic, [expectedMessage], options);
  }

  /**
   * Uses {@link PubSubFixture.expectMessages} to check that the given events have been published to the specified
   * topic. Each element in `expectedEvents` is the payload of a message, i.e. the `event` property.
   * If `attributes` are provided, all events are expected to share those attributes.
   *
   * @param topic The original name of the event topic.
   * @param expectedEvents The events expected to have been published.
   * @param options Options for the expectation.
   */
  async expectEvents(
    topic: string,
    expectedEvents: any[],
    options: ExpectEventOptions = {},
  ): Promise<void> {
    const attributes = expect.objectContaining(options.attributes ?? {});
    await this.expectMessages(
      topic,
      expectedEvents.map((event) =>
        expect.objectContaining({ event, attributes }),
      ),
      options,
    );
  }

  /**
   * Uses {@link PubSubFixture.expectMessage} to check that the given event has been published to the specified
   * topic. The `expectedEvent` is the payload of the message, i.e. the `event` property.
   *
   * @param topic The original name of the event topic.
   * @param expectedEvent The event expected to have been published.
   * @param options Options for the expectation.
   */
  async expectEvent(
    topic: string,
    expectedEvent: any,
    options: ExpectEventOptions = {},
  ): Promise<void> {
    await this.expectEvents(topic, [expectedEvent], options);
  }

  /**
   * Checks that no message has been published to the given topic.
   * By default, because publishing (and receiving) the messages is asynchronous, a small delay is added before checking
   * that no message has been received. This delay can be removed or increased by passing the `delay` option.
   *
   * @param topic The original name of the topic, i.e. the one used in production.
   * @param options Options for the expectation.
   */
  async expectNoMessage(
    topic: string,
    options: {
      /**
       * The delay (in milliseconds) before checking that no message has been received.
       */
      delay?: number;
    } = {},
  ): Promise<void> {
    const delay = options.delay ?? DEFAULT_EXPECT_NO_MESSAGE_DELAY;
    if (delay > 0) {
      await setTimeout(delay);
    }

    const numMessages = this.getReceivedMessages(topic).length;
    if (numMessages > 0) {
      throw new Error(
        `Expected 0 messages in '${topic}' but found ${numMessages}.`,
      );
    }
  }

  async expectNoEvent(topic: string): Promise<void> {
    await this.expectNoMessage(topic);
  }

  async clear(): Promise<void> {
    Object.values(this.topics).forEach((t) => {
      t.messages = [];
    });
  }

  /**
   * Deletes the temporary topic for the corresponding "production" topic.
   *
   * @param topic The original name of the topic, i.e. the one used in production.
   */
  async deleteTopic(topic: string): Promise<void> {
    const topicFixture = this.topics[topic];
    if (!topicFixture) {
      return;
    }

    delete this.topics[topic];

    topicFixture.subscription.removeAllListeners();
    await topicFixture.subscription.delete();

    await topicFixture.topic.delete();
  }

  async delete(): Promise<void> {
    await Promise.all(Object.keys(this.topics).map((t) => this.deleteTopic(t)));

    await this.pubSub.close();

    this.appFixture = undefined as any;
  }
}
