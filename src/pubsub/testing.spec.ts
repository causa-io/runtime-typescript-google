import { AppFixture } from '@causa/runtime/nestjs/testing';
import type { Topic } from '@google-cloud/pubsub';
import { jest } from '@jest/globals';
import { Module } from '@nestjs/common';
import 'jest-extended';
import { setTimeout } from 'timers/promises';
import { PubSubPublisher } from './publisher.js';
import { PubSubPublisherModule } from './publisher.module.js';
import { PubSubFixture } from './testing.js';

class SimpleEvent {
  constructor(data: Partial<SimpleEvent> = {}) {
    Object.assign(this, data);
  }

  value!: string;
}

@Module({ imports: [PubSubPublisherModule.forRoot()] })
class MyModule {}

describe('PubSubFixture', () => {
  // Test coverage for the `PubSubFixture` is spread across Pub/Sub-related test files, e.g. `publisher.spec.ts`.

  let appFixture: AppFixture;
  let fixture: PubSubFixture;
  let publisher: PubSubPublisher;

  beforeEach(async () => {
    fixture = new PubSubFixture({ 'my.event.v1': SimpleEvent });
    appFixture = new AppFixture(MyModule, { fixtures: [fixture] });
    await appFixture.init();
    publisher = appFixture.get(PubSubPublisher);
  });

  afterEach(() => appFixture.delete());

  describe('createWithOverrider', () => {
    it('should create temporary topics and return an overrider', async () => {
      const expectedTopicName = fixture.topics['my.event.v1'].topic.name;

      const actualPublisher = appFixture.get(PubSubPublisher);
      const actualTopic = (actualPublisher as any).getTopic('my.event.v1');

      expect(actualTopic.name).toEqual(expectedTopicName);
      expect(actualTopic.name).not.toContain('{{projectId}}');
    });
  });

  describe('expectEvents', () => {
    it('should match multiple events published to the same topic', async () => {
      const event1 = new SimpleEvent({ value: 'first' });
      const event2 = new SimpleEvent({ value: 'second' });
      const event3 = new SimpleEvent({ value: 'third' });
      await publisher.publish('my.event.v1', event1, {
        attributes: { source: 'test' },
      });
      await publisher.publish('my.event.v1', event2, {
        attributes: { source: 'test' },
      });
      await publisher.publish('my.event.v1', event3);

      await fixture.expectEvents(
        'my.event.v1',
        [
          { value: expect.toStartWith('fi') },
          { value: expect.toEndWith('ond') },
        ],
        { attributes: { source: 'test' } },
      );
    });

    it('should match events regardless of order', async () => {
      const event1 = new SimpleEvent({ value: 'first' });
      const event2 = new SimpleEvent({ value: 'second' });
      await publisher.publish('my.event.v1', event1);
      await publisher.publish('my.event.v1', event2);

      await fixture.expectEvents(
        'my.event.v1',
        [{ value: 'second' }, { value: 'first' }],
        { exact: true },
      );
    });

    it('should fail when an expected event is missing', async () => {
      const event1 = new SimpleEvent({ value: 'first' });
      await publisher.publish('my.event.v1', event1);

      const actual = fixture.expectEvents(
        'my.event.v1',
        [{ value: 'first' }, { value: 'missing' }],
        { timeout: 200 },
      );

      await expect(actual).rejects.toThrow();
    });

    it('should fail when exact is true and there are extra messages', async () => {
      const event1 = new SimpleEvent({ value: 'first' });
      const event2 = new SimpleEvent({ value: 'second' });
      const event3 = new SimpleEvent({ value: 'third' });
      await publisher.publish('my.event.v1', event3);
      await publisher.publish('my.event.v1', event1);
      await publisher.publish('my.event.v1', event2);

      const actual = fixture.expectEvents(
        'my.event.v1',
        [{ value: 'first' }, { value: 'second' }],
        { exact: true },
      );

      await expect(actual).rejects.toThrow();
    });
  });

  describe('clear', () => {
    it('should wait for all the published messages', async () => {
      // `getTopic` creates the `Topic` in the publisher's cache.
      const topic = (publisher as any).getTopic('my.event.v1') as Topic;
      // The first message is slower to be published, such that it is received after the second one.
      const publishMessage = topic.publishMessage.bind(topic);
      jest
        .spyOn(topic, 'publishMessage')
        .mockImplementationOnce(async (message) => {
          await setTimeout(500);
          return await publishMessage(message);
        });
      const publishPromises = ['a', 'b'].map((value) =>
        publisher.publish('my.event.v1', new SimpleEvent({ value })),
      );

      await fixture.clear();

      // Messages published before the `clear` should not leak into the messages published afterwards.
      await Promise.all(publishPromises);
      await publisher.publish('my.event.v1', new SimpleEvent({ value: 'c' }));
      await fixture.expectEvents('my.event.v1', [{ value: 'c' }], {
        exact: true,
      });
    });

    it('should not wait for messages that failed to be published', async () => {
      await publisher.publish('my.event.v1', new SimpleEvent({ value: 'a' }));
      await fixture.expectEvent('my.event.v1', { value: 'a' });
      jest
        .spyOn(publisher['topicCache']['my.event.v1'] as any, 'publishMessage')
        .mockRejectedValueOnce(new Error('📫💥'));
      const failedPublish = publisher.publish(
        'my.event.v1',
        new SimpleEvent({ value: 'b' }),
      );
      await expect(failedPublish).rejects.toThrow('📫💥');

      // This would time out and throw if the failed publish was expected to be received.
      await fixture.clear();

      await fixture.expectNoMessage('my.event.v1');
    });
  });
});
