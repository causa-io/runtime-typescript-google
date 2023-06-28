import { Event, JsonObjectSerializer, ObjectSerializer } from '@causa/runtime';
import { HttpStatus, INestApplication } from '@nestjs/common';
import supertest from 'supertest';
import * as uuid from 'uuid';

/**
 * A function that makes a query to an endpoint handling Pub/Sub events and tests the response.
 * By default, the `expectedStatus` is `200`, or the value provided to {@link makePubSubRequester}.
 */
export type EventRequester<T extends Event> = (
  endpoint: string,
  event: T,
  expectedStatus?: number,
) => Promise<void>;

/**
 * Creates an {@link EventRequester} for a NestJS HTTP application handling Pub/Sub messages.
 *
 * @param app The NestJS application handling events.
 * @param options Options when creating the requester.
 * @returns The {@link EventRequester}.
 */
export function makePubSubRequester<T extends Event>(
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
): EventRequester<T> {
  const request = supertest(app.getHttpServer());
  const routePrefix = options.routePrefix ?? '';
  const serializer = options.serializer ?? new JsonObjectSerializer();

  return async (endpoint, event, expectedStatus) => {
    const messageId = uuid.v4();
    const publishTime = new Date().toISOString();
    const buffer = await serializer.serialize(event);
    const data = buffer.toString('base64');

    await request
      .post(`${routePrefix}${endpoint}`)
      .send({
        message: {
          messageId,
          message_id: messageId,
          publishTime,
          publish_time: publishTime,
          attributes: {
            producedAt: event.producedAt,
            eventName: event.name,
            eventId: event.id,
          },
          data,
        },
        subscription: 'subscription',
      })
      .expect(expectedStatus ?? options.expectedStatus ?? HttpStatus.OK);
  };
}
