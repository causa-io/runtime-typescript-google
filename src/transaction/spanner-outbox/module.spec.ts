import {
  EVENT_PUBLISHER_INJECTION_NAME,
  LoggerModule,
} from '@causa/runtime/nestjs';
import { createMockConfigService } from '@causa/runtime/nestjs/testing';
import { Global, Module, type DynamicModule, type Type } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { PubSubPublisherModule } from '../../pubsub/index.js';
import { SpannerModule, SpannerTable } from '../../spanner/index.js';
import { SpannerOutboxEvent } from './event.js';
import {
  SpannerOutboxTransactionModule,
  type SpannerOutboxTransactionModuleOptions,
} from './module.js';
import { SpannerOutboxTransactionRunner } from './runner.js';
import { SpannerOutboxSender } from './sender.js';

describe('SpannerOutboxTransactionModule', () => {
  let testModule: TestingModule;
  let runner: SpannerOutboxTransactionRunner;
  let sender: SpannerOutboxSender;

  afterEach(async () => {
    await testModule?.close();
  });

  async function createModule(
    options: {
      config?: Record<string, any>;
      options?: SpannerOutboxTransactionModuleOptions;
      publisherModule?: Type<any> | DynamicModule;
    } = {},
  ): Promise<void> {
    testModule = await Test.createTestingModule({
      imports: [
        LoggerModule.forRoot(),
        ConfigModule.forRoot({ isGlobal: true }),
        SpannerModule.forRoot(),
        options.publisherModule ?? PubSubPublisherModule.forRoot(),
        SpannerOutboxTransactionModule.forRoot(options.options),
      ],
    })
      .overrideProvider(ConfigService)
      .useValue(
        createMockConfigService({
          SPANNER_INSTANCE: 'my-instance',
          SPANNER_DATABASE: 'my-database',
          ...options.config,
        }),
      )
      .compile();

    runner = testModule.get(SpannerOutboxTransactionRunner);
    sender = testModule.get(SpannerOutboxSender);
  }

  it('should configure the module with default options', async () => {
    @Global()
    @Module({
      providers: [{ provide: EVENT_PUBLISHER_INJECTION_NAME, useValue: {} }],
      exports: [EVENT_PUBLISHER_INJECTION_NAME],
    })
    class MyPublisherModule {}

    await createModule({ publisherModule: MyPublisherModule });

    expect(runner.outboxEventType).toBe(SpannerOutboxEvent);
    expect(runner.sender).toBe(sender);
    expect(sender.outboxEventType).toBe(SpannerOutboxEvent);
    expect(sender.batchSize).toBe(100);
    expect(sender.pollingInterval).toBe(10000);
    expect(sender.idColumn).toBe('id');
    expect(sender.leaseExpirationColumn).toBe('leaseExpiration');
    expect(sender.publishedAtColumn).toBe('publishedAt');
    expect(sender.index).toBeUndefined();
    expect(sender.sharding).toBeUndefined();
    expect(sender.leaseDuration).toBe(30000);
  });

  it('should change the default lease duration when the publisher is Pub/Sub', async () => {
    await createModule();

    expect(sender.leaseDuration).toBe(70000);
  });

  it('should get the configuration from the configuration service', async () => {
    await createModule({
      config: {
        SPANNER_OUTBOX_BATCH_SIZE: '10',
        SPANNER_OUTBOX_POLLING_INTERVAL: '0',
        SPANNER_OUTBOX_ID_COLUMN: 'myId',
        SPANNER_OUTBOX_LEASE_EXPIRATION_COLUMN: 'myLease',
        SPANNER_OUTBOX_PUBLISHED_AT_COLUMN: 'myPublished',
        SPANNER_OUTBOX_INDEX: 'MyIndex',
        SPANNER_OUTBOX_SHARDING_COLUMN: 'myColumn',
        SPANNER_OUTBOX_SHARDING_COUNT: '5',
        SPANNER_OUTBOX_LEASE_DURATION: '60000',
      },
    });

    expect(runner.outboxEventType).toBe(SpannerOutboxEvent);
    expect(runner.sender).toBe(sender);
    expect(sender.outboxEventType).toBe(SpannerOutboxEvent);
    expect(sender.batchSize).toBe(10);
    expect(sender.pollingInterval).toBe(0);
    expect(sender.idColumn).toBe('myId');
    expect(sender.leaseExpirationColumn).toBe('myLease');
    expect(sender.publishedAtColumn).toBe('myPublished');
    expect(sender.index).toBe('MyIndex');
    expect(sender.sharding).toEqual({
      column: 'myColumn',
      count: 5,
    });
    expect(sender.leaseDuration).toBe(60000);
  });

  it('should use the passed options', async () => {
    @SpannerTable({ primaryKey: ['id'] })
    class MyEvent implements SpannerOutboxEvent {
      readonly id!: string;
      readonly topic!: string;
      readonly data!: Buffer;
      readonly attributes!: Record<string, string>;
      readonly leaseExpiration!: Date | null;
      readonly publishedAt!: Date | null;
    }

    await createModule({
      options: {
        batchSize: 8,
        pollingInterval: 40000,
        idColumn: 'myIdColumn',
        leaseExpirationColumn: 'myLeaseColumn',
        publishedAtColumn: 'myPublishedColumn',
        index: 'MyFetchIndex',
        sharding: {
          column: 'myShardColumn',
          count: 4,
        },
        leaseDuration: 28000,
        outboxEventType: MyEvent,
      },
    });

    expect(runner.outboxEventType).toBe(MyEvent);
    expect(runner.sender).toBe(sender);
    expect(sender.outboxEventType).toBe(MyEvent);
    expect(sender.batchSize).toBe(8);
    expect(sender.pollingInterval).toBe(40000);
    expect(sender.idColumn).toBe('myIdColumn');
    expect(sender.leaseExpirationColumn).toBe('myLeaseColumn');
    expect(sender.publishedAtColumn).toBe('myPublishedColumn');
    expect(sender.index).toBe('MyFetchIndex');
    expect(sender.sharding).toEqual({
      column: 'myShardColumn',
      count: 4,
    });
    expect(sender.leaseDuration).toBe(28000);
  });
});
