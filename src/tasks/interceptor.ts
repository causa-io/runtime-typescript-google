import { ValidationError, parseObject } from '@causa/runtime';
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
import type { IncomingHttpHeaders } from 'node:http';
import { CloudTasksInfo } from './cloud-tasks-info.js';
import type { RequestWithCloudTasksInfo } from './task-event-info.decorator.js';

/**
 * The ID of the Cloud Tasks event handler interceptor, that can passed to the `UseEventHandler` decorator.
 */
export const CLOUD_TASKS_EVENT_HANDLER_ID = 'google.cloudTasks';

/**
 * Parses the Cloud Tasks headers of a request.
 * The headers are checked manually rather than using `parseObject`, which would copy all the headers of the request.
 *
 * @param headers The headers of the request.
 * @returns The parsed {@link CloudTasksInfo}.
 */
function parseCloudTasksHeaders(headers: IncomingHttpHeaders): CloudTasksInfo {
  const messages: string[] = [];
  const fields = new Set<string>();
  const fail = (field: string, message: string) => {
    fields.add(field);
    messages.push(message);
  };
  const checkCount = (field: string, value: number) => {
    if (!Number.isInteger(value)) {
      fail(field, `${field} must be an integer number`);
    }
    if (!(value >= 0)) {
      fail(field, `${field} must not be less than 0`);
    }
  };

  const queueName = headers['x-cloudtasks-queuename'];
  if (typeof queueName !== 'string') {
    fail('queueName', 'queueName must be a string');
  }
  const taskName = headers['x-cloudtasks-taskname'];
  if (typeof taskName !== 'string') {
    fail('taskName', 'taskName must be a string');
  }
  const retryCount = parseInt(headers['x-cloudtasks-taskretrycount'] as string);
  checkCount('retryCount', retryCount);
  const executionCount = parseInt(
    headers['x-cloudtasks-taskexecutioncount'] as string,
  );
  checkCount('executionCount', executionCount);
  const eta = new Date(
    parseFloat(headers['x-cloudtasks-tasketa'] as string) * 1000,
  );
  if (isNaN(eta.getTime())) {
    fail('eta', 'eta must be a Date instance');
  }
  const rawPreviousResponse = headers['x-cloudtasks-taskpreviousresponse'];
  const previousResponse =
    rawPreviousResponse !== undefined
      ? parseInt(rawPreviousResponse as string)
      : undefined;
  if (previousResponse !== undefined && !Number.isInteger(previousResponse)) {
    fail('previousResponse', 'previousResponse must be an integer number');
  }
  const retryReason = headers['x-cloudtasks-taskretryreason'];
  if (retryReason !== undefined && typeof retryReason !== 'string') {
    fail('retryReason', 'retryReason must be a string');
  }

  if (messages.length > 0) {
    throw new ValidationError(messages, [...fields]);
  }

  return new CloudTasksInfo({
    queueName: queueName as string,
    taskName: taskName as string,
    retryCount,
    executionCount,
    eta,
    previousResponse,
    retryReason: retryReason as string | undefined,
  });
}

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
      const info = parseCloudTasksHeaders(request.headers);
      this.assignEventId(info.taskName);
      this.logger.debug('Successfully parsed Cloud Tasks request.');
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
