import type { AppFixture, Fixture } from '@causa/runtime/nestjs/testing';
import { HttpStatus } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import type { CloudTasksInfo } from './cloud-tasks-info.js';

/**
 * Options when making a request to an endpoint handling Cloud Tasks events using a {@link CloudTasksEventRequester}.
 */
export type CloudTasksEventRequesterOptions = {
  /**
   * The expected status code when making the request.
   * Default is `200`.
   */
  readonly expectedStatus?: number;

  /**
   * The information about the Cloud Tasks task to include in the request headers.
   * If not provided, default values will be used.
   */
  readonly taskInfo?: Partial<CloudTasksInfo>;
};

/**
 * A function that makes a query to an endpoint handling Cloud Tasks events and tests the response.
 */
export type CloudTasksEventRequester = (
  event: object,
  options?: CloudTasksEventRequesterOptions,
) => Promise<void>;

/**
 * A utility class for testing Cloud Tasks event handlers.
 */
export class CloudTasksFixture implements Fixture {
  /**
   * The parent {@link AppFixture}.
   */
  private appFixture!: AppFixture;

  async init(appFixture: AppFixture): Promise<undefined> {
    this.appFixture = appFixture;
  }

  /**
   * Creates a {@link CloudTasksEventRequester} for an endpoint handling Cloud Tasks events.
   *
   * @param endpoint The endpoint to query.
   * @param options Options when creating the requester.
   * @returns The {@link CloudTasksEventRequester}.
   */
  makeRequester(
    endpoint: string,
    options: CloudTasksEventRequesterOptions = {},
  ): CloudTasksEventRequester {
    return async (event, requestOptions) => {
      const taskInfo: CloudTasksInfo = {
        queueName: 'queueName',
        taskName: randomUUID(),
        retryCount: 0,
        executionCount: 0,
        eta: new Date(),
        ...options.taskInfo,
        ...requestOptions?.taskInfo,
      };

      const headers: Record<string, string> = {
        'x-cloudtasks-queuename': taskInfo.queueName,
        'x-cloudtasks-taskname': taskInfo.taskName,
        'x-cloudtasks-taskretrycount': String(taskInfo.retryCount),
        'x-cloudtasks-taskexecutioncount': String(taskInfo.executionCount),
        'x-cloudtasks-tasketa': (taskInfo.eta.getTime() / 1000).toFixed(3),
      };

      if (taskInfo.previousResponse !== undefined) {
        headers['x-cloudtasks-taskpreviousresponse'] = String(
          taskInfo.previousResponse,
        );
      }

      if (taskInfo.retryReason !== undefined) {
        headers['x-cloudtasks-taskretryreason'] = taskInfo.retryReason;
      }

      const expectedStatus =
        requestOptions?.expectedStatus ??
        options.expectedStatus ??
        HttpStatus.OK;

      await this.appFixture.request
        .post(endpoint)
        .set(headers)
        .send(event)
        .expect(expectedStatus);
    };
  }

  async clear(): Promise<void> {}

  async delete(): Promise<void> {
    this.appFixture = undefined as any;
  }
}
