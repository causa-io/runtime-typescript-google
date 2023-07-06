import { getLoggedInfos, spyOnLogger } from '@causa/runtime/logging/testing';
import { LoggerModule } from '@causa/runtime/nestjs';
import { createMockConfigService } from '@causa/runtime/nestjs/testing';
import { Database } from '@google-cloud/spanner';
import { Injectable } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import { SPANNER_SESSION_POOL_OPTIONS_FOR_SERVICE } from './database.js';
import { SpannerEntityManager } from './entity-manager.js';
import { SpannerModule } from './module.js';

@Injectable()
class MyService {
  constructor(
    readonly database: Database,
    readonly entityManager: SpannerEntityManager,
  ) {}
}

describe('SpannerModule', () => {
  beforeEach(() => {
    spyOnLogger();
  });

  it('should expose the Spanner database using the configuration', async () => {
    const { database: actualDatabase } = await createInjectedService();

    expect(actualDatabase).toBeInstanceOf(Database);
    expect(actualDatabase.formattedName_).toEqual(
      'projects/demo-causa/instances/my-instance/databases/my-database',
    );
    expect((actualDatabase.pool_ as any).options).toMatchObject(
      SPANNER_SESSION_POOL_OPTIONS_FOR_SERVICE,
    );
  });

  it('should use the provided pool options', async () => {
    const { database: actualDatabase } = await createInjectedService([
      { poolOptions: { max: 42 } },
    ]);

    expect((actualDatabase.pool_ as any).options).toMatchObject({ max: 42 });
  });

  it('should register the catcher for Spanner database errors', async () => {
    const { database } = await createInjectedService();

    database.emit('error', new Error('💥'));

    expect(
      getLoggedInfos({ predicate: (o) => o.error.includes('💥') }),
    ).toEqual([
      expect.objectContaining({
        message:
          'Uncaught Spanner database error. This might be due to the background keep alive mechanism running in an idle Cloud Function.',
      }),
    ]);
  });

  it('should expose the SpannerEntityManager', async () => {
    const { database, entityManager: actualEntityManager } =
      await createInjectedService();

    expect(actualEntityManager).toBeInstanceOf(SpannerEntityManager);
    expect(actualEntityManager.database).toBe(database);
  });

  async function createInjectedService(
    args: Parameters<(typeof SpannerModule)['forRoot']> = [],
  ): Promise<MyService> {
    const testModule = await Test.createTestingModule({
      providers: [MyService],
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        LoggerModule,
        SpannerModule.forRoot(...args),
      ],
    })
      .overrideProvider(ConfigService)
      .useValue(
        createMockConfigService({
          SPANNER_INSTANCE: 'my-instance',
          SPANNER_DATABASE: 'my-database',
        }),
      )
      .compile();

    return testModule.get(MyService);
  }
});
