import {
  type Event,
  IsDateType,
  JsonObjectSerializer,
  ValidateNestedType,
} from '@causa/runtime';
import { EventAttributes, EventBody, Logger } from '@causa/runtime/nestjs';
import { AppFixture, LoggingFixture } from '@causa/runtime/nestjs/testing';
import { getLoggedInfos } from '@causa/runtime/testing';
import { Controller, HttpCode, HttpStatus, Module, Post } from '@nestjs/common';
import { APP_INTERCEPTOR } from '@nestjs/core';
import { IsString } from 'class-validator';
import { PubSubEventHandlerInterceptor } from './interceptor.js';
import { PubSubEventPublishTime } from './publish-time.decorator.js';
import { type EventRequester, PubSubFixture } from './testing.js';

class MyData {
  constructor(data: Partial<MyData> = {}) {
    Object.assign(this, { someProp: '👋', ...data });
  }

  @IsString()
  someProp!: string;
}

class MyEvent implements Event {
  constructor(data: Partial<MyEvent> = {}) {
    Object.assign(this, {
      id: '1234',
      producedAt: new Date(),
      name: 'my-event',
      data: new MyData(),
      ...data,
    });
  }

  @IsString()
  id!: string;

  @IsDateType()
  producedAt!: Date;

  @IsString()
  name!: string;

  @ValidateNestedType(() => MyData)
  data!: MyData;
}

@Controller()
class MyController {
  constructor(private readonly logger: Logger) {}

  @Post()
  @HttpCode(HttpStatus.OK)
  async handleMyEvent(
    @EventBody() event: MyEvent,
    @EventAttributes() attributes: Record<string, string>,
    @PubSubEventPublishTime() publishTime: Date,
  ): Promise<void> {
    this.logger.info(
      {
        someProp: event.data.someProp,
        someAttribute: attributes.someAttribute,
        publishTime,
      },
      '🎉',
    );
  }
}

@Module({
  controllers: [MyController],
  providers: [
    {
      provide: APP_INTERCEPTOR,
      useClass: PubSubEventHandlerInterceptor.withSerializer(
        new JsonObjectSerializer(),
      ),
    },
  ],
})
class MyModule {}

describe('PubSubEventHandlerInterceptor', () => {
  let appFixture: AppFixture;
  let request: EventRequester;

  beforeAll(async () => {
    appFixture = new AppFixture(MyModule, {
      fixtures: [new PubSubFixture({})],
    });
    await appFixture.init();
    request = appFixture.get(PubSubFixture).makeRequester('/');
  });

  afterEach(() => appFixture.clear());

  afterAll(() => appFixture.delete());

  it('should return 400 when the payload is invalid', async () => {
    await appFixture.request.post('/').send({ nope: '🙅' }).expect(400);

    appFixture.get(LoggingFixture).expectErrors({
      message: 'Received invalid Pub/Sub message.',
      validationMessages: expect.arrayContaining([
        'message should not be null or undefined',
        'subscription must be a string',
      ]),
    });
  });

  it('should deserialize the message data and return it', async () => {
    const expectedDate = new Date();

    await request(new MyEvent(), {
      attributes: { someAttribute: '🌻' },
      publishTime: expectedDate,
    });

    expect(getLoggedInfos({ predicate: (o) => o.message === '🎉' })).toEqual([
      expect.objectContaining({
        someProp: '👋',
        someAttribute: '🌻',
        publishTime: expectedDate.toISOString(),
        eventId: '1234',
        pubSubMessageId: expect.any(String),
      }),
    ]);
  });

  it('should return 200 and log an error when the event is invalid', async () => {
    const event = new MyEvent({ data: new MyData({ someProp: 1234 as any }) });

    await request(event);

    appFixture.get(LoggingFixture).expectErrors({
      eventId: event.id,
      message: 'Received an invalid event.',
      validationMessages: ['someProp must be a string'],
    });
  });
});
