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
    useFactory: (configService: ConfigService, logger: Logger) => {
      const instanceName = configService.getOrThrow<string>('SPANNER_INSTANCE');
      const databaseName = configService.getOrThrow<string>('SPANNER_DATABASE');

      const database = new Spanner()
        .instance(instanceName)
        .database(
          databaseName,
          options.poolOptions ?? SPANNER_SESSION_POOL_OPTIONS_FOR_SERVICE,
        );

      catchSpannerDatabaseErrors(database, logger.logger);

      return database;
    },
    inject: [ConfigService, Logger],
  };
}

/**
 * A global module that provides the Spanner {@link Database} and the {@link SpannerEntityManager}.
 * The {@link ConfigService} and {@link Logger} should be globally available for this module to work.
 */
export class SpannerModule {
  /**
   * Create a global module that provides the Spanner {@link Database} and the {@link SpannerEntityManager}.
   * The {@link ConfigService} and {@link Logger} should be globally available for this module to work.
   *
   * @param options Options for Spanner services.
   * @returns The module.
   */
  static forRoot(options: DatabaseOptions = {}): DynamicModule {
    return {
      module: SpannerModule,
      global: true,
      providers: [makeDatabaseProvider(options), SpannerEntityManager],
      exports: [Database, SpannerEntityManager],
    };
  }
}
