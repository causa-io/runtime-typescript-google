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
import { CloudTasksInfo } from './cloud-tasks-info.js';
import type { RequestWithCloudTasksInfo } from './task-event-info.decorator.js';

/**
 * The ID of the Cloud Tasks event handler interceptor, that can passed to the `UseEventHandler` decorator.
 */
export const CLOUD_TASKS_EVENT_HANDLER_ID = 'google.cloudTasks';

/**
 * The interceptor that should be added to controllers handling Cloud Tasks events.
 */
@Injectable()
export class CloudTasksEventHandlerInterceptor extends BaseEventHandlerInterceptor {
  constructor(
    reflector: Reflector,
    logger: Logger,
    options: EventHandlerInterceptorOptions = {},
  ) {
    super(CLOUD_TASKS_EVENT_HANDLER_ID, reflector, logger, options);
    this.logger.setContext(CloudTasksEventHandlerInterceptor.name);
  }

  /**
   * Parses the Cloud Tasks request, extracting task information from headers.
   *
   * @param request The express request object.
   * @returns The parsed Cloud Tasks information.
   */
  protected async parseCloudTasksRequest(
    request: Request,
  ): Promise<CloudTasksInfo> {
    try {
      const info = await parseObject(CloudTasksInfo, request.headers, {
        ...validatorOptions,
        forbidNonWhitelisted: false,
      });
      this.assignEventId(info.taskName);
      this.logger.info('Successfully parsed Cloud Tasks request.');
      return info;
    } catch (error: any) {
      this.logger.error(
        {
          error: error.stack,
          ...(error instanceof ValidationError
            ? { validationMessages: error.validationMessages }
            : {}),
        },
        'Received invalid Cloud Tasks request.',
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
      .getRequest<Request & RequestWithCloudTasksInfo>();

    request.cloudTasksInfo = await this.parseCloudTasksRequest(request);

    return await this.wrapParsing(async () => {
      const body = await parseObject(dataType, request.body, {
        forbidNonWhitelisted: false,
      });
      return { attributes: {}, body };
    });
  }

  /**
   * Creates a Cloud Tasks event handler interceptor class with the given options.
   *
   * @param options Options for the interceptor.
   * @returns The Cloud Tasks event handler interceptor class.
   */
  static withOptions(
    options: EventHandlerInterceptorOptions,
  ): Type<CloudTasksEventHandlerInterceptor> {
    @Injectable()
    class CloudTasksEventHandlerInterceptorWithOptions extends CloudTasksEventHandlerInterceptor {
      constructor(reflector: Reflector, logger: Logger) {
        super(reflector, logger, options);
      }
    }

    return CloudTasksEventHandlerInterceptorWithOptions;
  }
}
