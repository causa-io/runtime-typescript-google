import { EventPublisher, JsonObjectSerializer } from '@causa/runtime';
import { InjectEventPublisher, LoggerModule } from '@causa/runtime/nestjs';
import { createMockConfigService } from '@causa/runtime/nestjs/testing';
import { Topic } from '@google-cloud/pubsub';
import { Controller } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import { PinoLogger } from 'nestjs-pino';
import { PubSubPublisher } from './publisher.js';
import { PubSubPublisherModule } from './publisher.module.js';

@Controller()
class MyController {
  constructor(
    @InjectEventPublisher()
    readonly publisher: EventPublisher,
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
    const logger = await testModule.resolve(PinoLogger);

    const actualPublisher = testModule.get(MyController).publisher;

    expect(actualPublisher).toBeInstanceOf(PubSubPublisher);
    expect((actualPublisher as any).serializer).toBeInstanceOf(
      JsonObjectSerializer,
    );
    expect((actualPublisher as any).publishOptions).toBeUndefined();
    expect((actualPublisher as any).logger).toBe(logger.logger);
    const actualTopic = (actualPublisher as any).getTopic('my.topic.v1');
    expect(actualTopic).toBeInstanceOf(Topic);
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
});