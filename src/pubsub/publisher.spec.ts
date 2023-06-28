import { Event } from '@causa/runtime';
import { getLoggedErrors, spyOnLogger } from '@causa/runtime/logging/testing';
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
    configuration = await fixture.create('my.awesome-topic.v1', MyEvent);
    spyOnLogger();
  });

  beforeEach(async () => {
    publisher = new PubSubPublisher({
      configurationGetter: (key) => configuration[key],
    });
  });

  afterEach(() => {
    fixture.clear();
  });

  afterAll(async () => {
    await fixture.deleteAll();
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

    it('should log the message to publish when publishing fails', async () => {
      const event = new MyEvent({
        id: '1234',
        producedAt: new Date(),
        name: 'my-event',
        data: new MyData({ someProp: 'hello' }),
      });
      // This creates the `my.awesome-topic.v1` `Topic` in the cache.
      await publisher.publish('my.awesome-topic.v1', new MyEvent());
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

      expect(
        publisher['topicCache']['my.awesome-topic.v1'].flush,
      ).toHaveBeenCalledOnce();
    });
  });
});
