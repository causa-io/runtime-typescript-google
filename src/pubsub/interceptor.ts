import {
  AllowMissing,
  IsDateType,
  ValidateNestedType,
  ValidationError,
  parseObject,
} from '@causa/runtime';
import {
  BadRequestError,
  BaseEventHandlerInterceptor,
  ParsedEventRequest,
} from '@causa/runtime/nestjs';
import { ExecutionContext, Injectable } from '@nestjs/common';
import { IsBase64, IsObject, IsString } from 'class-validator';

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
  protected async parseEventFromContext(
    context: ExecutionContext,
  ): Promise<ParsedEventRequest> {
    const request = context.switchToHttp().getRequest();

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

    return { body, attributes: message.attributes };
  }
}
