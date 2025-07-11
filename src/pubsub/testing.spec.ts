import { AppFixture } from '@causa/runtime/nestjs/testing';
import { Module } from '@nestjs/common';
import { PubSubPublisher } from './publisher.js';
import { PubSubPublisherModule } from './publisher.module.js';
import { PubSubFixture } from './testing.js';

@Module({ imports: [PubSubPublisherModule.forRoot()] })
class MyModule {}

describe('PubSubFixture', () => {
  // Test coverage for the `PubSubFixture` is spread across Pub/Sub-related test files, e.g. `publisher.spec.ts`.

  let appFixture: AppFixture;
  let fixture: PubSubFixture;

  beforeEach(async () => {
    fixture = new PubSubFixture({ 'my.event.v1': class MyEvent {} });
    appFixture = new AppFixture(MyModule, { fixtures: [fixture] });
    await appFixture.init();
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
});
