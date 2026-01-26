import { type ExecutionContext, createParamDecorator } from '@nestjs/common';
import type { CloudTasksInfo } from './cloud-tasks-info.js';

/**
 * Additional information expected to be present on an express request that was parsed as a Cloud Tasks request.
 */
export type RequestWithCloudTasksInfo = {
  /**
   * Information about the Cloud Tasks task.
   */
  cloudTasksInfo: CloudTasksInfo;
};

/**
 * Decorates a route handler's parameter to populate it with information about the Cloud Tasks task.
 */
export const CloudTasksEventInfo = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext) => {
    const request = ctx.switchToHttp().getRequest<RequestWithCloudTasksInfo>();
    return request.cloudTasksInfo;
  },
);
