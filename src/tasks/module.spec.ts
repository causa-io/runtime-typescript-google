import { CloudTasksClient } from '@google-cloud/tasks';
import { Controller } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import { CloudTasksModule } from './module.js';
import { CloudTasksScheduler } from './scheduler.js';

@Controller()
class MyController {
  constructor(
    readonly client: CloudTasksClient,
    readonly scheduler: CloudTasksScheduler,
  ) {}
}

describe('CloudTasksModule', () => {
  it('should expose the CloudTasksClient and CloudTasksScheduler', async () => {
    const testModule = await Test.createTestingModule({
      controllers: [MyController],
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        CloudTasksModule.forRoot(),
      ],
    }).compile();

    const { client: actualClient, scheduler: actualScheduler } =
      testModule.get(MyController);

    expect(actualClient).toBeInstanceOf(CloudTasksClient);
    expect(actualScheduler).toBeInstanceOf(CloudTasksScheduler);
  });
});
