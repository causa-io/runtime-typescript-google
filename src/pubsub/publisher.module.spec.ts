import { EventPublisher, JsonObjectSerializer } from '@causa/runtime';
import {
  EVENT_PUBLISHER_INJECTION_NAME,
  InjectEventPublisher,
  Logger,
  LoggerModule,
} from '@causa/runtime/nestjs';
import { createMockConfigService } from '@causa/runtime/nestjs/testing';
import { Topic } from '@google-cloud/pubsub';
import { jest } from '@jest/globals';
import { Controller } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import 'jest-extended';
import { PubSubPublisher } from './publisher.js';
import { PubSubPublisherModule } from './publisher.module.js';

@Controller()
class MyController {
  constructor(
    @InjectEventPublisher()
    readonly publisher: EventPublisher,
    readonly typedPubSubPublisher: PubSubPublisher,
  ) {}
}

describe('PubSubPublisherModule', () => {
  it('should expose the PubSubPublisher as the event publisher', async () => {
    const testModule = await Test.createTestingModule({
      controllers: [MyController],
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        LoggerModule,
        PubSubPublisherModule.forRoot(),
      ],
    })
      .overrideProvider(ConfigService)
      .useValue(
        createMockConfigService({
          PUBSUB_TOPIC_MY_TOPIC_V1: 'projects/my-project/topics/my-topic',
        }),
      )
      .compile();
    const logger = await testModule.resolve(Logger);

    const actualPublisher = testModule.get(MyController).publisher;

    expect(actualPublisher).toBeInstanceOf(PubSubPublisher);
    expect((actualPublisher as any).serializer).toBeInstanceOf(
      JsonObjectSerializer,
    );
    expect((actualPublisher as any).publishOptions).toBeUndefined();
    expect((actualPublisher as any).logger).toBe(logger.logger);
    const actualTopic = (actualPublisher as any).getTopic('my.topic.v1');
    expect(actualTopic).toBeInstanceOf(Topic);
    expect(actualTopic.name).toEqual('projects/my-project/topics/my-topic');
  });

  it('should expose the PubSubPublisher as is', async () => {
    const testModule = await Test.createTestingModule({
      controllers: [MyController],
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        LoggerModule,
        PubSubPublisherModule.forRoot(),
      ],
    }).compile();

    const { publisher, typedPubSubPublisher } = testModule.get(MyController);

    expect(typedPubSubPublisher).toBe(publisher);
  });

  it('should customize the serializer and publish options', async () => {
    const expectedSerializer = {} as any;
    const testModule = await Test.createTestingModule({
      controllers: [MyController],
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        LoggerModule,
        PubSubPublisherModule.forRoot({
          serializer: expectedSerializer,
          publishOptions: { batching: { maxMessages: 1 } },
        }),
      ],
    }).compile();

    const actualPublisher = testModule.get(MyController).publisher;

    expect(actualPublisher).toBeInstanceOf(PubSubPublisher);
    expect((actualPublisher as any).serializer).toBe(expectedSerializer);
    expect((actualPublisher as any).publishOptions).toEqual({
      batching: { maxMessages: 1 },
    });
  });

  it('should flush the publisher on application shutdown', async () => {
    const testModule = await Test.createTestingModule({
      controllers: [MyController],
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        LoggerModule,
        PubSubPublisherModule.forRoot(),
      ],
    }).compile();
    const publisher: PubSubPublisher = testModule.get(
      EVENT_PUBLISHER_INJECTION_NAME,
    );
    jest.spyOn(publisher, 'flush');

    await testModule.close();

    expect(publisher.flush).toHaveBeenCalledOnce();
  });
});
