import { Database, Spanner } from '@google-cloud/spanner';
import type { DynamicModule, FactoryProvider } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { SpannerEntityManager } from './entity-manager.js';
import { SpannerLifecycleService } from './lifecycle.service.js';

/**
 * Creates a NestJS factory provider for the Spanner {@link Database}.
 *
 * @returns The NestJS provider for the Spanner {@link Database}.
 */
function makeDatabaseProvider(): FactoryProvider<Database> {
  return {
    provide: Database,
    useFactory: (configService: ConfigService, spanner: Spanner) => {
      const instanceName = configService.getOrThrow<string>('SPANNER_INSTANCE');
      const databaseName = configService.getOrThrow<string>('SPANNER_DATABASE');

      return spanner.instance(instanceName).database(databaseName);
    },
    inject: [ConfigService, Spanner],
  };
}

/**
 * A global module that provides the Spanner {@link Database} and the {@link SpannerEntityManager}.
 * The {@link ConfigService} and {@link Logger} should be globally available for this module to work.
 */
export class SpannerModule {
  /**
   * Creates a global module that provides the {@link Spanner} client, the {@link Database} and the
   * {@link SpannerEntityManager}.
   * The {@link ConfigService} and {@link Logger} should be globally available for this module to work.
   *
   * @returns The module.
   */
  static forRoot(): DynamicModule {
    return {
      module: SpannerModule,
      global: true,
      providers: [
        { provide: Spanner, useFactory: () => new Spanner() },
        makeDatabaseProvider(),
        SpannerEntityManager,
        SpannerLifecycleService,
      ],
      exports: [Spanner, Database, SpannerEntityManager],
    };
  }
}
