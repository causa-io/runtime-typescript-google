import { Logger } from '@causa/runtime/nestjs';
import { Database, Spanner } from '@google-cloud/spanner';
import { SessionPoolOptions } from '@google-cloud/spanner/build/src/session-pool.js';
import { DynamicModule, FactoryProvider } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  SPANNER_SESSION_POOL_OPTIONS_FOR_SERVICE,
  catchSpannerDatabaseErrors,
} from './database.js';
import { SpannerEntityManager } from './entity-manager.js';
import { SpannerLifecycleService } from './lifecycle.service.js';

/**
 * Options when instantiating the Spanner {@link Database}.
 */
type DatabaseOptions = {
  /**
   * {@link SessionPoolOptions} to use when instantiating the database.
   * Default is {@link SPANNER_SESSION_POOL_OPTIONS_FOR_SERVICE}.
   */
  poolOptions?: SessionPoolOptions;
};

/**
 * Creates a NestJS factory provider for the Spanner {@link Database}.
 *
 * @param options Options when instantiating the Spanner {@link Database}.
 * @returns The NestJS provider for the Spanner {@link Database}.
 */
function makeDatabaseProvider(
  options: DatabaseOptions = {},
): FactoryProvider<Database> {
  return {
    provide: Database,
    useFactory: (
      configService: ConfigService,
      spanner: Spanner,
      logger: Logger,
    ) => {
      const instanceName = configService.getOrThrow<string>('SPANNER_INSTANCE');
      const databaseName = configService.getOrThrow<string>('SPANNER_DATABASE');

      const database = spanner
        .instance(instanceName)
        .database(
          databaseName,
          options.poolOptions ?? SPANNER_SESSION_POOL_OPTIONS_FOR_SERVICE,
        );

      catchSpannerDatabaseErrors(database, logger.logger);

      return database;
    },
    inject: [ConfigService, Spanner, Logger],
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
   * @param options Options for Spanner services.
   * @returns The module.
   */
  static forRoot(options: DatabaseOptions = {}): DynamicModule {
    return {
      module: SpannerModule,
      global: true,
      providers: [
        { provide: Spanner, useFactory: () => new Spanner() },
        makeDatabaseProvider(options),
        SpannerEntityManager,
        SpannerLifecycleService,
      ],
      exports: [Spanner, Database, SpannerEntityManager],
    };
  }
}
