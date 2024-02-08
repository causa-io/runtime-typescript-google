import { ExecutionContext, createParamDecorator } from '@nestjs/common';
import { RequestWithPubSubInfo } from './request-with-pubsub-info.js';

/**
 * Decorates a route handler's parameter to populate it with the `publishTime` of the Pub/Sub message.
 */
export const PubSubEventPublishTime = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext) => {
    const request = ctx.switchToHttp().getRequest<RequestWithPubSubInfo>();
    return request.pubSubPublishTime;
  },
);
