import { ValidationError, parseObject, validatorOptions } from '@causa/runtime';
import {
  BadRequestErrorDto,
  BaseEventHandlerInterceptor,
  type EventHandlerInterceptorOptions,
  Logger,
  type ParsedEventRequest,
  throwHttpErrorResponse,
} from '@causa/runtime/nestjs';
import { type ExecutionContext, Injectable, type Type } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import type { Request } from 'express';
import { CloudSchedulerInfo } from './cloud-scheduler-info.js';
import type { RequestWithCloudSchedulerInfo } from './scheduler-event-info.decorator.js';

/**
 * The ID of the Cloud Scheduler event handler interceptor, that can be passed to the `UseEventHandler` decorator.
 */
export const CLOUD_SCHEDULER_EVENT_HANDLER_ID = 'google.cloudScheduler';

/**
 * The interceptor that should be added to controllers handling Cloud Scheduler events.
 *
 * Because Cloud Scheduler jobs are often configured to not send a body, the `@EventBody` parameter of route handlers
 * can be typed as a plain `object`. In that case, body parsing and validation are skipped, and an empty object is
 * returned.
 */
@Injectable()
export class CloudSchedulerEventHandlerInterceptor extends BaseEventHandlerInterceptor {
  constructor(
    reflector: Reflector,
    logger: Logger,
    options: EventHandlerInterceptorOptions = {},
  ) {
    super(CLOUD_SCHEDULER_EVENT_HANDLER_ID, reflector, logger, options);
    this.logger.setContext(CloudSchedulerEventHandlerInterceptor.name);
  }

  /**
   * Parses the Cloud Scheduler request, extracting job information from headers.
   *
   * @param request The express request object.
   * @returns The parsed Cloud Scheduler information.
   */
  protected async parseCloudSchedulerRequest(
    request: Request,
  ): Promise<CloudSchedulerInfo> {
    try {
      const info = await parseObject(CloudSchedulerInfo, request.headers, {
        ...validatorOptions,
        forbidNonWhitelisted: false,
      });
      this.logger.info('Successfully parsed Cloud Scheduler request.');
      return info;
    } catch (error: any) {
      this.logger.error(
        {
          error: error.stack,
          ...(error instanceof ValidationError
            ? { validationMessages: error.validationMessages }
            : {}),
        },
        'Received invalid Cloud Scheduler request.',
      );

      throwHttpErrorResponse(new BadRequestErrorDto());
    }
  }

  protected async parseEventFromContext(
    context: ExecutionContext,
    dataType: Type,
  ): Promise<ParsedEventRequest> {
    const request = context
      .switchToHttp()
      .getRequest<Request & RequestWithCloudSchedulerInfo>();

    request.cloudSchedulerInfo = await this.parseCloudSchedulerRequest(request);

    // This supports `@EventBody()` decorating a plain object type. This can be useful as bodies are often not needed
    // for Cloud Scheduler jobs.
    if (dataType === Object) {
      return { attributes: {}, body: {} };
    }

    return await this.wrapParsing(async () => {
      const body = await parseObject(dataType, request.body, {
        forbidNonWhitelisted: false,
      });
      return { attributes: {}, body };
    });
  }

  /**
   * Creates a Cloud Scheduler event handler interceptor class with the given options.
   *
   * @param options Options for the interceptor.
   * @returns The Cloud Scheduler event handler interceptor class.
   */
  static withOptions(
    options: EventHandlerInterceptorOptions,
  ): Type<CloudSchedulerEventHandlerInterceptor> {
    @Injectable()
    class CloudSchedulerEventHandlerInterceptorWithOptions extends CloudSchedulerEventHandlerInterceptor {
      constructor(reflector: Reflector, logger: Logger) {
        super(reflector, logger, options);
      }
    }

    return CloudSchedulerEventHandlerInterceptorWithOptions;
  }
}
