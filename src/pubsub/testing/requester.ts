import { JsonObjectSerializer, ObjectSerializer } from '@causa/runtime';
import { HttpStatus, INestApplication } from '@nestjs/common';
import supertest from 'supertest';
import * as uuid from 'uuid';

/**
 * Options when making a request to an endpoint handling Pub/Sub events using an {@link EventRequester}.
 */
export type EventRequesterOptions = {
  /**
   * The attributes to add to the Pub/Sub message.
   * Using `undefined` values allows removing the attributes set by default.
   */
  attributes?: Record<string, string | undefined>;

  /**
   * The expected status code when making the request.
   * Default is `200`.
   */
  expectedStatus?: number;

  /**
   * The time to set as the publication time of the Pub/Sub message.
   */
  publishTime?: Date;
};

/**
 * A function that makes a query to an endpoint handling Pub/Sub events and tests the response.
 */
export type EventRequester = (
  endpoint: string,
  event: object,
  options?: EventRequesterOptions,
) => Promise<void>;

/**
 * Creates an {@link EventRequester} for a NestJS HTTP application handling Pub/Sub messages.
 * If the `event` passed to the {@link EventRequester} conforms to the `Event` interface (if it has `producedAt`, `name`
 * and / or `id` properties), the default attributes are set in the Pub/Sub message. Default attributes can be
 * overridden (or removed by passing `undefined`) using {@link EventRequesterOptions.attributes}.
 *
 * @param app The NestJS application handling events.
 * @param options Options when creating the requester.
 * @returns The {@link EventRequester}.
 */
export function makePubSubRequester(
  app: INestApplication,
  options: {
    /**
     * The prefix added to all routes handling events.
     * Used when constructing the request to the HTTP application.
     */
    routePrefix?: string;

    /**
     * The serializer to use to serialize events.
     */
    serializer?: ObjectSerializer;

    /**
     * The default expected status code when making a request.
     */
    expectedStatus?: number;
  } = {},
): EventRequester {
  const request = supertest(app.getHttpServer());
  const routePrefix = options.routePrefix ?? '';
  const serializer = options.serializer ?? new JsonObjectSerializer();

  return async (endpoint, event, requestOptions) => {
    const messageId = uuid.v4();
    const publishTime = (
      requestOptions?.publishTime ?? new Date()
    ).toISOString();
    const buffer = await serializer.serialize(event);
    const data = buffer.toString('base64');

    // Default attributes if the event conforms to the `Event` interface.
    const defaultAttributes: Record<string, string> = {};
    if ('producedAt' in event && event.producedAt instanceof Date) {
      defaultAttributes.producedAt = event.producedAt.toISOString();
    }
    if ('name' in event && typeof event.name === 'string') {
      defaultAttributes.eventName = event.name;
    }
    if ('id' in event && typeof event.id === 'string') {
      defaultAttributes.eventId = event.id;
    }
    const attributes = { ...defaultAttributes, ...requestOptions?.attributes };

    await request
      .post(`${routePrefix}${endpoint}`)
      .send({
        message: {
          messageId,
          message_id: messageId,
          publishTime,
          publish_time: publishTime,
          attributes,
          data,
        },
        subscription: 'subscription',
      })
      .expect(
        requestOptions?.expectedStatus ??
          options.expectedStatus ??
          HttpStatus.OK,
      );
  };
}
