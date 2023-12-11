import { JsonObjectSerializer, ObjectSerializer } from '@causa/runtime';
import { Logger } from '@causa/runtime/nestjs';
import { DynamicModule } from '@nestjs/common';
import { APP_INTERCEPTOR, Reflector } from '@nestjs/core';
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

      /**
       * Whether to set the Pub/Sub event handler interceptor as a global NestJS app interceptor.
       * Defaults to `true`.
       */
      setAppInterceptor?: boolean;
    } = {},
  ): DynamicModule {
    const serializer = options.serializer ?? new JsonObjectSerializer();

    return {
      module: PubSubEventHandlerModule,
      global: true,
      providers: [
        {
          provide: PubSubEventHandlerInterceptor,
          useFactory: (reflector: Reflector, logger: Logger) =>
            new PubSubEventHandlerInterceptor(serializer, reflector, logger),
          inject: [Reflector, Logger],
        },
        ...(options.setAppInterceptor ?? true
          ? [
              {
                provide: APP_INTERCEPTOR,
                useExisting: PubSubEventHandlerInterceptor,
              },
            ]
          : []),
      ],
    };
  }
}
