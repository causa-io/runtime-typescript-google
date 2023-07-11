import { createApp } from '@causa/runtime/nestjs';
import { makeTestAppFactory } from '@causa/runtime/nestjs/testing';
import { INestApplication, Module } from '@nestjs/common';
import { PubSubPublisher } from '../publisher.js';
import { PubSubPublisherModule } from '../publisher.module.js';
import { PubSubFixture } from './fixture.js';

describe('PubSubFixture', () => {
  // Test coverage for the `PubSubFixture` is spread across Pub/Sub-related test files, e.g. `publisher.spec.ts`.

  let fixture: PubSubFixture;

  beforeEach(() => {
    fixture = new PubSubFixture();
  });

  describe('createWithOverrider', () => {
    @Module({ imports: [PubSubPublisherModule.forRoot()] })
    class MyModule {}

    it('should create temporary topics and return an overrider', async () => {
      const actualOverrider = await fixture.createWithOverrider({
        'my.event.v1': class MyEvent {},
      });
      const expectedTopicName = fixture.fixtures['my.event.v1'].topic.name;

      let app: INestApplication | undefined;
      try {
        app = await createApp(MyModule, {
          appFactory: makeTestAppFactory({ overrides: actualOverrider }),
        });

        const actualPublisher = app.get(PubSubPublisher);
        const actualTopic = (actualPublisher as any).getTopic('my.event.v1');
        expect(actualTopic.name).toEqual(expectedTopicName);
      } finally {
        await app?.close();
        await fixture.deleteAll();
      }
    });
  });
});
