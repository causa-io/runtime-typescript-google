import { JsonObjectSerializer, ObjectSerializer } from '@causa/runtime';
import { DynamicModule } from '@nestjs/common';
import { APP_INTERCEPTOR, Reflector } from '@nestjs/core';
import { PinoLogger } from 'nestjs-pino';
import { PubSubEventHandlerInterceptor } from './interceptor.js';

/**
 * A module that provides an interceptor for controllers handling Pub/Sub events.
 */
export class PubSubEventHandlerModule {
  /**
   * Creates a module that provides an interceptor for controllers handling Pub/Sub events.
   * The interceptor is set up globally.
   *
   * @param options Options for the Pub/Sub event handler interceptor.
   * @returns The module.
   */
  static forRoot(
    options: {
      /**
       * The {@link ObjectSerializer} to use to deserialize events.
       */
      serializer?: ObjectSerializer;
    } = {},
  ): DynamicModule {
    const serializer = options.serializer ?? new JsonObjectSerializer();

    return {
      module: PubSubEventHandlerModule,
      providers: [
        {
          provide: APP_INTERCEPTOR,
          useFactory: (reflector, logger) =>
            new PubSubEventHandlerInterceptor(serializer, reflector, logger),
          inject: [Reflector, PinoLogger],
        },
      ],
    };
  }
}
