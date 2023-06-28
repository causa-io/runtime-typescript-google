import { EVENT_PUBLISHER_INJECTION_NAME } from '@causa/runtime/nestjs';
import { DynamicModule } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PinoLogger } from 'nestjs-pino';
import { PubSubPublisher, PubSubPublisherOptions } from './publisher.js';

/**
 * A NestJS module that provides a {@link PubSubPublisher} as the event publisher.
 */
export class PubSubPublisherModule {
  /**
   * Create a global module that provides a {@link PubSubPublisher} as the event publisher.
   *
   * @param options Options for the {@link PubSubPublisher}.
   * @returns The module.
   */
  static forRoot(
    options: Pick<PubSubPublisherOptions, 'publishOptions' | 'serializer'> = {},
  ): DynamicModule {
    return {
      module: PubSubPublisherModule,
      providers: [
        {
          provide: EVENT_PUBLISHER_INJECTION_NAME,
          useFactory: (configService: ConfigService, logger: PinoLogger) =>
            new PubSubPublisher({
              ...options,
              configurationGetter: (key) => configService.get(key),
              logger: logger.logger,
            }),
          inject: [ConfigService, PinoLogger],
        },
      ],
      exports: [EVENT_PUBLISHER_INJECTION_NAME],
      global: true,
    };
  }
}
