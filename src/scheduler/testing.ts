import type { AppFixture, Fixture } from '@causa/runtime/nestjs/testing';
import { HttpStatus } from '@nestjs/common';
import type { CloudSchedulerInfo } from './cloud-scheduler-info.js';

/**
 * Options when making a request to an endpoint handling Cloud Scheduler events using a {@link CloudSchedulerEventRequester}.
 */
export type CloudSchedulerEventRequesterOptions = {
  /**
   * The expected status code when making the request.
   * Default is `200`.
   */
  readonly expectedStatus?: number;

  /**
   * The information about the Cloud Scheduler job to include in the request headers.
   * If not provided, default values will be used.
   */
  readonly jobInfo?: Partial<CloudSchedulerInfo>;
};

/**
 * A function that makes a query to an endpoint handling Cloud Scheduler events and tests the response.
 */
export type CloudSchedulerEventRequester = (
  event?: object,
  options?: CloudSchedulerEventRequesterOptions,
) => Promise<void>;

/**
 * A utility class for testing Cloud Scheduler event handlers.
 */
export class CloudSchedulerFixture implements Fixture {
  /**
   * The parent {@link AppFixture}.
   */
  private appFixture!: AppFixture;

  async init(appFixture: AppFixture): Promise<undefined> {
    this.appFixture = appFixture;
  }

  /**
   * Creates a {@link CloudSchedulerEventRequester} for an endpoint handling Cloud Scheduler events.
   *
   * @param endpoint The endpoint to query.
   * @param options Options when creating the requester.
   * @returns The {@link CloudSchedulerEventRequester}.
   */
  makeRequester(
    endpoint: string,
    options: CloudSchedulerEventRequesterOptions = {},
  ): CloudSchedulerEventRequester {
    return async (event, requestOptions) => {
      const jobInfo: CloudSchedulerInfo = {
        jobName: 'jobName',
        ...options.jobInfo,
        ...requestOptions?.jobInfo,
      };

      const headers: Record<string, string> = {
        'x-cloudscheduler-jobname': jobInfo.jobName,
      };

      if (jobInfo.scheduleTime !== undefined) {
        headers['x-cloudscheduler-scheduletime'] =
          jobInfo.scheduleTime.toISOString();
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
