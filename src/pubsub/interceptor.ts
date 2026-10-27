import {
  type ObjectSerializer,
  ValidationError,
  validateObject,
} from '@causa/runtime';
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
import type { RequestWithPubSubInfo } from './request-with-pubsub-info.js';

/**
 * The ID of the Pub/Sub event handler interceptor, that can passed to the `UseEventHandler` decorator.
 */
export const PUBSUB_EVENT_HANDLER_ID = 'google.pubSub';

/**
 * A Pub/Sub message, parsed from the payload of a push request.
 * This is not exposed to handlers.
 */
type PubSubMessage = {
  /**
   * The ID of the Pub/Sub message.
   */
  readonly messageId: string;

  /**
   * The date at which Pub/Sub received and published the message.
   */
  readonly publishTime: Date;

  /**
   * Attributes associated with the message.
   */
  readonly attributes?: Record<string, string>;

  /**
   * The data within the message, as a Base64 string.
   */
  readonly data: string;

  /**
   * The data of the Pub/Sub message as a `Buffer` instead of a Base64 string.
   */
  readonly body: Buffer;
};

/**
 * Checks whether the given value is an object, and not an array.
 *
 * @param value The value to check.
 * @returns `true` if the value is an object.
 */
function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Parses the payload of a Pub/Sub push request.
 * The payload has a fixed shape defined by Pub/Sub. It is checked manually rather than using `parseObject`, which would
 * copy it and run a regular expression over the entire data. Data that is not valid Base64 is not detected here, but
 * makes the deserialization of the event fail.
 *
 * @param payload The payload of the push request.
 * @returns The parsed Pub/Sub message.
 */
function parsePushPayload(payload: unknown): PubSubMessage {
  const messages: string[] = [];
  const fields: string[] = [];
  const fail = (field: string, message: string) => {
    fields.push(field);
    messages.push(message);
  };

  const { message, subscription } = isObject(payload) ? payload : {};
  if (typeof subscription !== 'string') {
    fail('subscription', 'subscription must be a string');
  }
  if (!isObject(message)) {
    fail(
      'message',
      message === undefined || message === null
        ? 'message should not be null or undefined'
        : 'nested property message must be either object or array',
    );
    throw new ValidationError(messages, fields);
  }

  const { messageId, publishTime, attributes, data } = message;
  if (typeof messageId !== 'string') {
    fail('message.messageId', 'messageId must be a string');
  }
  const date =
    typeof publishTime === 'string' || typeof publishTime === 'number'
      ? new Date(publishTime)
      : undefined;
  if (!date || isNaN(date.getTime())) {
    fail('message.publishTime', 'publishTime must be a Date instance');
  }
  if (attributes !== undefined && !isObject(attributes)) {
    fail('message.attributes', 'attributes must be an object');
  }
  if (typeof data !== 'string') {
    fail('message.data', 'data must be a string');
  }

  if (messages.length > 0) {
    throw new ValidationError(messages, fields);
  }

  return {
    messageId: messageId as string,
    publishTime: date as Date,
    attributes: attributes as Record<string, string> | undefined,
    data: data as string,
    body: Buffer.from(data as string, 'base64'),
  };
}

/**
 * The interceptor that should be added to controllers handling Pub/Sub events.
 */
@Injectable()
export class PubSubEventHandlerInterceptor extends BaseEventHandlerInterceptor {
  constructor(
    protected readonly serializer: ObjectSerializer,
    reflector: Reflector,
    logger: Logger,
    options: EventHandlerInterceptorOptions = {},
  ) {
    super(PUBSUB_EVENT_HANDLER_ID, reflector, logger, options);
    this.logger.setContext(PubSubEventHandlerInterceptor.name);
  }

  /**
   * Parses the given request as the payload of a Pub/Sub push request.
   *
   * @param request The express request object.
   * @returns The parsed Pub/Sub message.
   */
  protected async parsePubSubMessage(request: Request): Promise<PubSubMessage> {
    let message: PubSubMessage;
    try {
      message = parsePushPayload(request.body);
    } catch (error: any) {
      this.logger.error(
        {
          error: error.stack,
          ...(error instanceof ValidationError
            ? { validationMessages: error.validationMessages }
            : {}),
        },
        'Received invalid Pub/Sub message.',
      );

      throwHttpErrorResponse(new BadRequestErrorDto());
    }

    this.logger.assign({ pubSubMessageId: message.messageId });
    this.logger.debug('Successfully parsed Pub/Sub message.');

    return message;
  }

  protected async parseEventFromContext(
    context: ExecutionContext,
    dataType: Type,
  ): Promise<ParsedEventRequest> {
    const request = context
      .switchToHttp()
      .getRequest<Request & RequestWithPubSubInfo>();
    const message = await this.parsePubSubMessage(request);

    request.pubSubPublishTime = message.publishTime;

    return await this.wrapParsing(async () => {
      const body = await this.serializer.deserialize(dataType, message.body);

      if (body.id && typeof body.id === 'string') {
        this.assignEventId(body.id);
      }

      await validateObject(body, {
        forbidNonWhitelisted: false,
      });

      return { attributes: message.attributes ?? {}, body };
    });
  }

  /**
   * Returns a `PubSubEventHandlerInterceptor` class that uses the provided {@link ObjectSerializer}.
   * This can be used with the `UseInterceptors` decorator.
   *
   * @param serializer The {@link ObjectSerializer} to use to deserialize the event data.
   * @param options Options for the interceptor.
   * @returns A class that can be used as an interceptor for Pub/Sub event handlers.
   */
  static withSerializer(
    serializer: ObjectSerializer,
    options: EventHandlerInterceptorOptions = {},
  ): Type<PubSubEventHandlerInterceptor> {
    @Injectable()
    class PubSubEventHandlerInterceptorWithSerializer extends PubSubEventHandlerInterceptor {
      constructor(reflector: Reflector, logger: Logger) {
        super(serializer, reflector, logger, options);
      }
    }

    return PubSubEventHandlerInterceptorWithSerializer;
  }
}
