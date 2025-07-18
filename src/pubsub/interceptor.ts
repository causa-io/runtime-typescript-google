import {
  AllowMissing,
  IsDateType,
  type ObjectSerializer,
  ValidateNestedType,
  ValidationError,
  parseObject,
  validateObject,
} from '@causa/runtime';
import {
  BadRequestErrorDto,
  BaseEventHandlerInterceptor,
  Logger,
  type ParsedEventRequest,
  throwHttpErrorResponse,
} from '@causa/runtime/nestjs';
import { type ExecutionContext, Injectable, type Type } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { IsBase64, IsObject, IsString } from 'class-validator';
import type { Request } from 'express';
import type { RequestWithPubSubInfo } from './request-with-pubsub-info.js';

/**
 * The ID of the Pub/Sub event handler interceptor, that can passed to the `UseEventHandler` decorator.
 */
export const PUBSUB_EVENT_HANDLER_ID = 'google.pubSub';

/**
 * A Pub/Sub message.
 * This is used to ensure the input message has the correct format, but this is not exposed to handlers.
 */
class PubSubMessage {
  /**
   * The ID of the Pub/Sub message.
   */
  @IsString()
  readonly messageId!: string;

  /**
   * The date at which Pub/Sub received and published the message.
   */
  @IsDateType()
  readonly publishTime!: Date;

  /**
   * Attributes associated with the message.
   */
  @IsObject()
  @AllowMissing()
  readonly attributes?: Record<string, string>;

  /**
   * The data within the message.
   */
  @IsBase64()
  readonly data!: string;
}

/**
 * The payload of a push request by Pub/Sub.
 */
class PubSubMessagePayload {
  /**
   * The pushed message.
   */
  @ValidateNestedType(() => PubSubMessage)
  readonly message!: PubSubMessage;

  /**
   * The ID of the subscription for which the message is pushed.
   */
  @IsString()
  readonly subscription!: string;
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
  ) {
    super(PUBSUB_EVENT_HANDLER_ID, reflector, logger);
    this.logger.setContext(PubSubEventHandlerInterceptor.name);
  }

  /**
   * Parses the given request as the payload of a Pub/Sub push request.
   *
   * @param request The express request object.
   * @returns The parsed Pub/Sub message.
   */
  protected async parsePubSubMessage(request: Request): Promise<
    PubSubMessage & {
      /**
       * The data of the Pub/Sub message as a `Buffer` instead of a Base64 string.
       */
      body: Buffer;
    }
  > {
    let message: PubSubMessage;
    let body: Buffer;
    try {
      const payload = await parseObject(PubSubMessagePayload, request.body, {
        forbidNonWhitelisted: false,
      });

      message = payload.message;

      body = Buffer.from(message.data, 'base64');
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
    this.logger.info('Successfully parsed Pub/Sub message.');

    return { ...message, body };
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
   * @returns A class that can be used as an interceptor for Pub/Sub event handlers.
   */
  static withSerializer(
    serializer: ObjectSerializer,
  ): Type<PubSubEventHandlerInterceptor> {
    @Injectable()
    class PubSubEventHandlerInterceptorWithSerializer extends PubSubEventHandlerInterceptor {
      constructor(reflector: Reflector, logger: Logger) {
        super(serializer, reflector, logger);
      }
    }

    return PubSubEventHandlerInterceptorWithSerializer;
  }
}
