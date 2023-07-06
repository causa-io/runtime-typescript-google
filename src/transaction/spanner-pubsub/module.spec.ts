import { EventPublisher } from '@causa/runtime';
import { InjectEventPublisher, LoggerModule } from '@causa/runtime/nestjs';
import { createMockConfigService } from '@causa/runtime/nestjs/testing';
import { Injectable } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import { PubSubPublisher, PubSubPublisherModule } from '../../pubsub/index.js';
import { SpannerEntityManager, SpannerModule } from '../../spanner/index.js';
import { SpannerPubSubTransactionModule } from './module.js';
import { SpannerPubSubTransactionRunner } from './runner.js';

@Injectable()
class MyService {
  constructor(
    readonly entityManager: SpannerEntityManager,
    @InjectEventPublisher()
    readonly publisher: EventPublisher,
    readonly runner: SpannerPubSubTransactionRunner,
  ) {}
}

describe('SpannerPubSubTransactionModule', () => {
  it('should expose the runner', async () => {
    const testModule = await Test.createTestingModule({
      providers: [MyService],
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        LoggerModule,
        SpannerModule.forRoot(),
        PubSubPublisherModule.forRoot(),
        SpannerPubSubTransactionModule.forRoot(),
      ],
    })
      .overrideProvider(ConfigService)
      .useValue(
        createMockConfigService({
          SPANNER_INSTANCE: 'my-instance',
          SPANNER_DATABASE: 'my-database',
        }),
      )
      .compile();

    const { entityManager, runner: actualRunner } = testModule.get(MyService);

    expect(actualRunner).toBeInstanceOf(SpannerPubSubTransactionRunner);
    expect(actualRunner.entityManager).toBe(entityManager);
    expect(actualRunner.publisher).toBeInstanceOf(PubSubPublisher);
  });
});
