import { Event, IsDateType, ValidateNestedType } from '@causa/runtime';
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
  INestApplication,
  Module,
  Post,
} from '@nestjs/common';
import { IsString } from 'class-validator';
import supertest from 'supertest';
import { PubSubEventHandlerModule } from './interceptor.module.js';
import { EventRequester, makePubSubRequester } from './testing/index.js';

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
  ): Promise<void> {
    this.logger.info(
      {
        someProp: event.data.someProp,
        someAttribute: attributes.someAttribute,
      },
      '🎉',
    );
  }
}

@Module({
  controllers: [MyController],
  imports: [PubSubEventHandlerModule.forRoot()],
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
    await request('/', new MyEvent(), {
      expectedStatus: 200,
      attributes: { someAttribute: '🌻' },
    });

    expect(getLoggedInfos({ predicate: (o) => o.message === '🎉' })).toEqual([
      expect.objectContaining({
        someProp: '👋',
        someAttribute: '🌻',
        eventId: '1234',
        pubSubMessageId: expect.any(String),
      }),
    ]);
  });
});
