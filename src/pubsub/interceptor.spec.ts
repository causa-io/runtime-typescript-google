import {
  type Event,
  IsDateType,
  JsonObjectSerializer,
  ValidateNestedType,
} from '@causa/runtime';
import {
  EventAttributes,
  EventBody,
  Logger,
  createApp,
} from '@causa/runtime/nestjs';
import {
  getLoggedErrors,
  getLoggedInfos,
  spyOnLogger,
} from '@causa/runtime/testing';
import {
  Controller,
  HttpCode,
  HttpStatus,
  type INestApplication,
  Module,
  Post,
} from '@nestjs/common';
import { APP_INTERCEPTOR } from '@nestjs/core';
import { IsString } from 'class-validator';
import supertest from 'supertest';
import { PubSubEventHandlerInterceptor } from './interceptor.js';
import { PubSubEventPublishTime } from './publish-time.decorator.js';
import { type EventRequester, makePubSubRequester } from './testing/index.js';

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
  let app: INestApplication;
  let request: EventRequester;

  beforeAll(async () => {
    app = await createApp(MyModule, {});
    request = makePubSubRequester(app);
    spyOnLogger();
  });

  afterAll(async () => {
    await app.close();
  });

  it('should return 400 when the payload is invalid', async () => {
    await supertest(app.getHttpServer())
      .post('/')
      .send({ nope: '🙅' })
      .expect(400);

    expect(getLoggedErrors()).toEqual([
      expect.objectContaining({
        message: 'Received invalid Pub/Sub message.',
        validationMessages: expect.arrayContaining([
          'message should not be null or undefined',
          'subscription must be a string',
        ]),
      }),
    ]);
  });

  it('should deserialize the message data and return it', async () => {
    const expectedDate = new Date();

    await request('/', new MyEvent(), {
      expectedStatus: 200,
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

    await request('/', event, { expectedStatus: 200 });

    expect(getLoggedErrors()).toEqual([
      expect.objectContaining({
        eventId: event.id,
        message: 'Received an invalid event.',
        validationMessages: ['someProp must be a string'],
      }),
    ]);
  });
});
