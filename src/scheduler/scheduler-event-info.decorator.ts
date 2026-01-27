import { type ExecutionContext, createParamDecorator } from '@nestjs/common';
import type { CloudSchedulerInfo } from './cloud-scheduler-info.js';

/**
 * Additional information expected to be present on an express request that was parsed as a Cloud Scheduler request.
 */
export type RequestWithCloudSchedulerInfo = {
  /**
   * Information about the Cloud Scheduler job.
   */
  cloudSchedulerInfo: CloudSchedulerInfo;
};

/**
 * Decorates a route handler's parameter to populate it with information about the Cloud Scheduler job.
 */
export const CloudSchedulerEventInfo = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext) => {
    const request = ctx
      .switchToHttp()
      .getRequest<RequestWithCloudSchedulerInfo>();
    return request.cloudSchedulerInfo;
  },
);
