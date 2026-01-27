import { EventBody, Logger } from '@causa/runtime/nestjs';
import { AppFixture, LoggingFixture } from '@causa/runtime/nestjs/testing';
import { Controller, HttpCode, HttpStatus, Module, Post } from '@nestjs/common';
import { APP_INTERCEPTOR } from '@nestjs/core';
import { IsString } from 'class-validator';
import type { CloudSchedulerInfo } from './cloud-scheduler-info.js';
import { CloudSchedulerEventHandlerInterceptor } from './interceptor.js';
import { CloudSchedulerEventInfo as CloudSchedulerInfoDecorator } from './scheduler-event-info.decorator.js';
import {
  type CloudSchedulerEventRequester,
  CloudSchedulerFixture,
} from './testing.js';

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
    @CloudSchedulerInfoDecorator() jobInfo: CloudSchedulerInfo,
  ): Promise<void> {
    this.logger.info({ ...jobInfo, someProp: body.someProp }, '🎉');
  }

  @Post('empty')
  @HttpCode(HttpStatus.OK)
  async handleEmptyBody(
    @EventBody() body: object,
    @CloudSchedulerInfoDecorator() jobInfo: CloudSchedulerInfo,
  ): Promise<void> {
    this.logger.info({ jobName: jobInfo.jobName, body }, '🎉');
  }
}

@Module({
  controllers: [MyController],
  providers: [
    {
      provide: APP_INTERCEPTOR,
      useClass: CloudSchedulerEventHandlerInterceptor.withOptions({}),
    },
  ],
})
class MyModule {}

describe('CloudSchedulerEventHandlerInterceptor', () => {
  let appFixture: AppFixture;
  let request: CloudSchedulerEventRequester;

  beforeAll(async () => {
    appFixture = new AppFixture(MyModule, {
      fixtures: [new CloudSchedulerFixture()],
    });
    await appFixture.init();
    request = appFixture.get(CloudSchedulerFixture).makeRequester('/');
  });

  afterEach(() => appFixture.clear());

  afterAll(() => appFixture.delete());

  it('should return 400 when Cloud Scheduler headers are missing', async () => {
    await appFixture.request.post('/').send({ nope: '🙅' }).expect(400);

    appFixture.get(LoggingFixture).expectErrors({
      message: 'Received invalid Cloud Scheduler request.',
      validationMessages: expect.toIncludeSameMembers([
        'jobName must be a string',
      ]),
    });
  });

  it('should deserialize the request body and provide job info', async () => {
    await request(
      { someProp: '👋' },
      { jobInfo: { jobName: 'my-scheduled-job' } },
    );

    appFixture
      .get(LoggingFixture)
      .expectInfos(
        { message: '🎉', someProp: '👋', jobName: 'my-scheduled-job' },
        { exact: false },
      );
  });

  it('should include optional schedule time when present', async () => {
    const expectedScheduleTime = new Date();

    await request(
      { someProp: '👋' },
      {
        jobInfo: {
          jobName: 'my-scheduled-job',
          scheduleTime: expectedScheduleTime,
        },
      },
    );

    appFixture.get(LoggingFixture).expectInfos(
      {
        message: '🎉',
        someProp: '👋',
        jobName: 'my-scheduled-job',
        scheduleTime: expectedScheduleTime.toISOString(),
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

  it('should handle an empty body when the event type allows it', async () => {
    await appFixture.get(CloudSchedulerFixture).makeRequester('/empty')(
      undefined,
      { jobInfo: { jobName: 'my-scheduled-job' } },
    );

    appFixture
      .get(LoggingFixture)
      .expectInfos(
        { message: '🎉', jobName: 'my-scheduled-job', body: {} },
        { exact: false },
      );
  });
});
