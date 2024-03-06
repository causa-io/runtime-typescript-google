import { CloudTasksClient } from '@google-cloud/tasks';
import { DynamicModule } from '@nestjs/common';
import { CloudTasksScheduler } from './scheduler.js';

/**
 * The module exposing the Cloud Tasks client and scheduler.
 */
export class CloudTasksModule {
  /**
   * Create a global module that provides a {@link CloudTasksClient} and {@link CloudTasksScheduler}.
   *
   * @returns The module.
   */
  static forRoot(): DynamicModule {
    return {
      module: CloudTasksModule,
      global: true,
      providers: [
        {
          provide: CloudTasksClient,
          useFactory: () => new CloudTasksClient(),
        },
        CloudTasksScheduler,
      ],
      exports: [CloudTasksClient, CloudTasksScheduler],
    };
  }
}
