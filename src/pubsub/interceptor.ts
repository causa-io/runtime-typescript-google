import {
  AllowMissing,
  IsDateType,
  ObjectSerializer,
  ValidateNestedType,
  ValidationError,
  parseObject,
  validateObject,
} from '@causa/runtime';
import {
  BadRequestError,
  BaseEventHandlerInterceptor,
  ParsedEventRequest,
} from '@causa/runtime/nestjs';
import { ExecutionContext, Injectable, Type } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { IsBase64, IsObject, IsString } from 'class-validator';
import { Request } from 'express';
import { PinoLogger } from 'nestjs-pino';

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
    logger: PinoLogger,
  ) {
    super(PUBSUB_EVENT_HANDLER_ID, reflector, logger);
  }

  /**
   * Parses the given request as the payload of a Pub/Sub push request.
   *
   * @param request The express request object.
   * @returns The parsed Pub/Sub message.
   */
  protected async parsePubSubMessage(request: Request): Promise<{
    /**
     * The body of the Pub/Sub message.
     */
    body: Buffer;

    /**
     * The attributes of the Pub/Sub message.
     */
    attributes: Record<string, string>;
  }> {
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

      throw new BadRequestError();
    }

    this.logger.assign({ pubSubMessageId: message.messageId });
    this.logger.info('Successfully parsed Pub/Sub message.');

    return { body, attributes: message.attributes ?? {} };
  }

  protected async parseEventFromContext(
    context: ExecutionContext,
    dataType: Type,
  ): Promise<ParsedEventRequest> {
    const request = context.switchToHttp().getRequest<Request>();
    const message = await this.parsePubSubMessage(request);

    return await this.wrapParsing(async () => {
      const body = await this.serializer.deserialize(dataType, message.body);

      if (body.id && typeof body.id === 'string') {
        this.assignEventId(body.id);
      }

      await validateObject(body, {
        forbidNonWhitelisted: false,
      });

      return { attributes: message.attributes, body };
    });
  }
}
