import type { Event, PreparedEvent } from '@causa/runtime';
import { getLoggedErrors, spyOnLogger } from '@causa/runtime/testing';
import { Topic } from '@google-cloud/pubsub';
import { jest } from '@jest/globals';
import { Transform, Type } from 'class-transformer';
import 'jest-extended';
import { PubSubTopicNotConfiguredError } from './errors.js';
import { PubSubPublisher } from './publisher.js';
import { PubSubFixture } from './testing/index.js';

class MyData {
  constructor(data: Partial<MyData>) {
    Object.assign(this, { someProp: '👋', ...data });
  }

  @Transform(({ value }) => value.toUpperCase())
  someProp!: string;
}

class MyEvent implements Event {
  constructor(data: Partial<MyEvent> = {}) {
    Object.assign(this, {
      id: '1234',
      producedAt: new Date(),
      name: 'my-event',
      data: new MyData({ someProp: 'hello' }),
      ...data,
    });
  }

  id!: string;

  @Type(() => Date)
  producedAt!: Date;

  name!: string;

  @Type(() => MyData)
  data!: MyData;
}

describe('PubSubPublisher', () => {
  let fixture: PubSubFixture;
  let configuration: Record<string, string>;
  let publisher: PubSubPublisher;

  beforeAll(async () => {
    fixture = new PubSubFixture();
    configuration = await fixture.createMany({
      'my.awesome-topic.v1': MyEvent,
      'my.other-topic.v1': MyEvent,
    });
    spyOnLogger();
  });

  beforeEach(async () => {
    publisher = new PubSubPublisher({
      configurationGetter: (key) => configuration[key],
      topicPublishOptions: {
        'my.other-topic.v1': {
          batching: { maxMessages: 100, maxBytes: 1000 },
        },
      },
    });
  });

  afterEach(() => {
    fixture.clear();
  });

  afterAll(async () => {
    await fixture.deleteAll();
  });

  describe('constructor', () => {
    it('should default to disable batching', () => {
      expect(publisher.publishOptions).toMatchObject({
        batching: { maxMessages: 1 },
      });
    });

    it('should customize publish options per topic', () => {
      const actualMyAwesomeTopic: Topic = (publisher as any).getTopic(
        'my.awesome-topic.v1',
      );
      const actualMyOtherTopic: Topic = (publisher as any).getTopic(
        'my.other-topic.v1',
      );

      expect(actualMyAwesomeTopic.publisher.settings).toMatchObject({
        batching: { maxMessages: 1 },
      });
      expect(actualMyOtherTopic.publisher.settings).toMatchObject({
        batching: { maxMessages: 100, maxBytes: 1000 },
      });
    });
  });

  describe('prepare', () => {
    it('should serialize the event and add default attributes', async () => {
      const event = new MyEvent({
        id: '1234',
        producedAt: new Date(),
        name: 'my-event',
        data: new MyData({ someProp: 'HELLO' }),
      });

      const actual = await publisher.prepare('my.awesome-topic.v1', event);

      expect(actual).toEqual({
        topic: 'my.awesome-topic.v1',
        data: Buffer.from(JSON.stringify(event)),
        attributes: {
          eventId: '1234',
          producedAt: event.producedAt.toISOString(),
          eventName: 'my-event',
        },
        key: undefined,
      });
    });

    it('should ignore non-existing or invalid properties for default attributes', async () => {
      const event = {
        id: true,
        producedAt: '📅',
      };

      const actual = await publisher.prepare('my.awesome-topic.v1', event, {
        attributes: { custom: '🎉' },
      });

      expect(actual).toEqual({
        topic: 'my.awesome-topic.v1',
        data: Buffer.from(JSON.stringify(event)),
        attributes: { custom: '🎉' },
        key: undefined,
      });
    });
  });

  describe('publish', () => {
    it('should fail to publish an event if the topic is not configured', async () => {
      const actualPromise = publisher.publish('my.unknown.topic', {} as any);

      await expect(actualPromise).rejects.toThrow(
        PubSubTopicNotConfiguredError,
      );
    });

    it('should serialize and publish an event', async () => {
      const event = new MyEvent({
        id: '1234',
        producedAt: new Date(),
        name: 'my-event',
        data: new MyData({ someProp: 'hello' }),
      });

      await publisher.publish('my.awesome-topic.v1', event);

      await fixture.expectMessageInTopic('my.awesome-topic.v1', {
        attributes: {
          producedAt: event.producedAt.toISOString(),
          eventName: 'my-event',
          eventId: '1234',
        },
        orderingKey: undefined,
        event: {
          id: '1234',
          producedAt: event.producedAt,
          name: 'my-event',
          data: { someProp: 'HELLO' },
        },
      });
      // Tests the same thing using the simpler `expectEventInTopic`.
      await fixture.expectEventInTopic(
        'my.awesome-topic.v1',
        {
          id: '1234',
          producedAt: event.producedAt,
          name: 'my-event',
          data: { someProp: 'HELLO' },
        },
        { attributes: { eventName: 'my-event' } },
      );
    });

    it('should publish an event with custom attributes and key', async () => {
      const event = new MyEvent({
        id: '1234',
        producedAt: new Date(),
        name: 'my-event',
        data: new MyData({ someProp: 'hello' }),
      });

      await publisher.publish('my.awesome-topic.v1', event, {
        attributes: { custom: '🎉' },
        key: '🔑',
      });

      await fixture.expectMessageInTopic('my.awesome-topic.v1', {
        attributes: {
          producedAt: event.producedAt.toISOString(),
          eventName: 'my-event',
          eventId: '1234',
          custom: '🎉',
        },
        orderingKey: '🔑',
        event: {
          id: '1234',
          producedAt: event.producedAt,
          name: 'my-event',
          data: { someProp: 'HELLO' },
        },
      });
    });

    it('should support events that do not conform to the Event interface', async () => {
      const event = { someData: '🗃️' };

      await publisher.publish('my.awesome-topic.v1', event);

      await fixture.expectMessageInTopic('my.awesome-topic.v1', {
        attributes: {},
        orderingKey: undefined,
        // Not an equal match because the `MyEvent` constructor assigns default values to other fields.
        event: expect.objectContaining(event),
      });
    });

    it('should publish a prepare event', async () => {
      const event = new MyEvent({
        id: '1234',
        producedAt: new Date(),
        name: 'my-event',
        data: new MyData({ someProp: 'HELLO' }),
      });
      const preparedEvent: PreparedEvent = {
        topic: 'my.awesome-topic.v1',
        data: Buffer.from(JSON.stringify(event)),
        attributes: { someAttributes: '🏷️' },
      };

      await publisher.publish(preparedEvent);

      await fixture.expectMessageInTopic('my.awesome-topic.v1', {
        attributes: { someAttributes: '🏷️' },
        orderingKey: undefined,
        event,
      });
    });

    it('should log the message to publish when publishing fails', async () => {
      const event = new MyEvent({
        id: '1234',
        producedAt: new Date(),
        name: 'my-event',
        data: new MyData({ someProp: 'hello' }),
      });
      // This creates the `my.awesome-topic.v1` `Topic` in the cache.
      await publisher.publish('my.awesome-topic.v1', new MyEvent());
      await fixture.expectMessageInTopic(
        'my.awesome-topic.v1',
        expect.anything(),
      );
      fixture.clear();
      jest
        .spyOn(
          publisher['topicCache']['my.awesome-topic.v1'] as any,
          'publishMessage',
        )
        .mockRejectedValue(new Error('📫💥'));

      const actualPromise = publisher.publish('my.awesome-topic.v1', event);

      await expect(actualPromise).rejects.toThrow('📫💥');

      await fixture.expectNoMessageInTopic('my.awesome-topic.v1');
      expect(getLoggedErrors()).toEqual([
        expect.objectContaining({
          message: 'Failed to publish message to Pub/Sub.',
          topic: 'my.awesome-topic.v1',
          eventId: '1234',
          pubSubTopic: configuration['PUBSUB_TOPIC_MY_AWESOME_TOPIC_V1'],
          pubSubMessage: Buffer.from(
            JSON.stringify({
              id: '1234',
              producedAt: event.producedAt,
              name: 'my-event',
              data: { someProp: 'HELLO' },
            }),
          ).toString('base64'),
          pubSubAttributes: {
            producedAt: event.producedAt.toISOString(),
            eventName: 'my-event',
            eventId: '1234',
          },
        }),
      ]);
    });
  });

  describe('flush', () => {
    it('should flush all topics', async () => {
      await publisher.publish('my.awesome-topic.v1', new MyEvent({}));
      jest.spyOn(publisher['topicCache']['my.awesome-topic.v1'], 'flush');

      await publisher.flush();

      await fixture.expectMessageInTopic(
        'my.awesome-topic.v1',
        expect.anything(),
      );
      expect(
        publisher['topicCache']['my.awesome-topic.v1'].flush,
      ).toHaveBeenCalledOnce();
    });
  });
});
