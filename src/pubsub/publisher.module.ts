import { EVENT_PUBLISHER_INJECTION_NAME, Logger } from '@causa/runtime/nestjs';
import { PubSub } from '@google-cloud/pubsub';
import { DynamicModule } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PubSubPublisher, PubSubPublisherOptions } from './publisher.js';

/**
 * The name of the injection key used to provide the {@link PubSubPublisherOptions.configurationGetter}.
 */
export const PUBSUB_PUBLISHER_CONFIGURATION_GETTER_INJECTION_NAME =
  'CAUSA_PUBSUB_PUBLISHER_CONFIGURATION_GETTER';

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
        { provide: PubSub, useValue: new PubSub() },
        {
          provide: PUBSUB_PUBLISHER_CONFIGURATION_GETTER_INJECTION_NAME,
          useFactory: (configService: ConfigService) => (key: string) =>
            configService.get(key),
          inject: [ConfigService],
        },
        {
          provide: PubSubPublisher,
          useFactory: (
            pubSub: PubSub,
            configurationGetter: (key: string) => string | undefined,
            { logger }: Logger,
          ) =>
            new PubSubPublisher({
              ...options,
              pubSub,
              configurationGetter,
              logger,
            }),
          inject: [
            PubSub,
            PUBSUB_PUBLISHER_CONFIGURATION_GETTER_INJECTION_NAME,
            Logger,
          ],
        },
        {
          provide: EVENT_PUBLISHER_INJECTION_NAME,
          useExisting: PubSubPublisher,
        },
      ],
      exports: [PubSub, PubSubPublisher, EVENT_PUBLISHER_INJECTION_NAME],
      global: true,
    };
  }
}
