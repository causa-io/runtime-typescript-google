import { EventBody, Logger } from '@causa/runtime/nestjs';
import { AppFixture, LoggingFixture } from '@causa/runtime/nestjs/testing';
import { Controller, HttpCode, HttpStatus, Module, Post } from '@nestjs/common';
import { APP_INTERCEPTOR } from '@nestjs/core';
import { IsString } from 'class-validator';
import type { CloudTasksInfo } from './cloud-tasks-info.js';
import { CloudTasksEventHandlerInterceptor } from './interceptor.js';
import { CloudTasksEventInfo as CloudTasksInfoDecorator } from './task-event-info.decorator.js';
import { type CloudTasksEventRequester, CloudTasksFixture } from './testing.js';

class MyBody {
  @IsString()
  someProp!: string;
}

@Controller()
class MyController {
  constructor(private readonly logger: Logger) {}

  @Post()
  @HttpCode(HttpStatus.OK)
  async handleBody(
    @EventBody() body: MyBody,
    @CloudTasksInfoDecorator() taskInfo: CloudTasksInfo,
  ): Promise<void> {
    this.logger.info({ ...taskInfo, someProp: body.someProp }, '🎉');
  }
}

@Module({
  controllers: [MyController],
  providers: [
    {
      provide: APP_INTERCEPTOR,
      useClass: CloudTasksEventHandlerInterceptor.withOptions({}),
    },
  ],
})
class MyModule {}

describe('CloudTasksEventHandlerInterceptor', () => {
  let appFixture: AppFixture;
  let request: CloudTasksEventRequester;

  beforeAll(async () => {
    appFixture = new AppFixture(MyModule, {
      fixtures: [new CloudTasksFixture()],
    });
    await appFixture.init();
    request = appFixture.get(CloudTasksFixture).makeRequester('/');
  });

  afterEach(() => appFixture.clear());

  afterAll(() => appFixture.delete());

  it('should return 400 when Cloud Tasks headers are missing', async () => {
    await appFixture.request.post('/').send({ nope: '🙅' }).expect(400);

    appFixture.get(LoggingFixture).expectErrors({
      message: 'Received invalid Cloud Tasks request.',
      validationMessages: expect.toIncludeSameMembers([
        'queueName must be a string',
        'taskName must be a string',
        'retryCount must not be less than 0',
        'retryCount must be an integer number',
        'executionCount must not be less than 0',
        'executionCount must be an integer number',
        'eta must be a Date instance',
      ]),
    });
  });

  it('should return 400 when Cloud Tasks headers are invalid', async () => {
    await appFixture.request
      .post('/')
      .set('x-cloudtasks-taskretrycount', 'not-a-number')
      .set('x-cloudtasks-taskexecutioncount', 'nope')
      .set('x-cloudtasks-tasketa', 'still-nope')
      .send({ nope: '🙅' })
      .expect(400);

    appFixture.get(LoggingFixture).expectErrors({
      message: 'Received invalid Cloud Tasks request.',
      validationMessages: expect.toIncludeSameMembers([
        'queueName must be a string',
        'taskName must be a string',
        'retryCount must not be less than 0',
        'retryCount must be an integer number',
        'executionCount must not be less than 0',
        'executionCount must be an integer number',
        'eta must be a Date instance',
      ]),
    });
  });

  it('should deserialize the request body and provide task info', async () => {
    const expectedEta = new Date();

    await request(
      { someProp: '👋' },
      {
        taskInfo: {
          queueName: 'my-queue',
          taskName: 'my-task',
          retryCount: 2,
          executionCount: 3,
          eta: expectedEta,
        },
      },
    );

    appFixture.get(LoggingFixture).expectInfos(
      {
        message: '🎉',
        someProp: '👋',
        queueName: 'my-queue',
        taskName: 'my-task',
        retryCount: 2,
        executionCount: 3,
        eta: expectedEta.toISOString(),
        eventId: 'my-task',
      },
      { exact: false },
    );
  });

  it('should include optional retry information when present', async () => {
    await request(
      { someProp: '👋' },
      {
        taskInfo: {
          previousResponse: 500,
          retryReason: 'Internal Server Error',
        },
      },
    );

    appFixture.get(LoggingFixture).expectInfos(
      {
        message: '🎉',
        someProp: '👋',
        previousResponse: 500,
        retryReason: 'Internal Server Error',
      },
      { exact: false },
    );
  });

  it('should return 200 and log an error when the body is invalid', async () => {
    await request({ someProp: 1234 });

    appFixture.get(LoggingFixture).expectErrors({
      message: 'Received an invalid event.',
      validationMessages: ['someProp must be a string'],
    });
  });
});
