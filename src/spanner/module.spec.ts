import { LoggerModule } from '@causa/runtime/nestjs';
import { createMockConfigService } from '@causa/runtime/nestjs/testing';
import {
  getLoggedErrors,
  getLoggedInfos,
  spyOnLogger,
} from '@causa/runtime/testing';
import { Database, Spanner } from '@google-cloud/spanner';
import { SessionLeakError } from '@google-cloud/spanner/build/src/session-pool.js';
import { jest } from '@jest/globals';
import { Injectable } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import 'jest-extended';
import { SPANNER_SESSION_POOL_OPTIONS_FOR_SERVICE } from './database.js';
import { SpannerEntityManager } from './entity-manager.js';
import { SpannerModule } from './module.js';

@Injectable()
class MyService {
  constructor(
    readonly spanner: Spanner,
    readonly database: Database,
    readonly entityManager: SpannerEntityManager,
  ) {}
}

describe('SpannerModule', () => {
  let testModule: TestingModule | undefined;

  beforeEach(() => {
    spyOnLogger();
  });

  afterEach(async () => {
    await testModule?.close();
    testModule = undefined;
  });

  it('should expose the Spanner client', async () => {
    const { spanner: actualSpanner } = await createInjectedService();

    expect(actualSpanner).toBeInstanceOf(Spanner);
  });

  it('should expose the Spanner database using the configuration', async () => {
    const { database: actualDatabase, spanner } = await createInjectedService();

    expect(actualDatabase).toBeInstanceOf(Database);
    expect(actualDatabase.formattedName_).toEqual(
      'projects/demo-causa/instances/my-instance/databases/my-database',
    );
    expect((actualDatabase.pool_ as any).options).toMatchObject(
      SPANNER_SESSION_POOL_OPTIONS_FOR_SERVICE,
    );
    const databaseInstance = (actualDatabase as any).parent;
    const databaseSpanner = (databaseInstance as any).parent;
    expect(databaseSpanner).toBe(spanner);
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

  it('should close the Spanner client and database when closing the app', async () => {
    const { database, spanner } = await createInjectedService();
    jest.spyOn(spanner, 'close');
    jest.spyOn(database, 'close');

    await testModule?.close();

    expect(database.close).toHaveBeenCalledExactlyOnceWith();
    expect(spanner.close).toHaveBeenCalledExactlyOnceWith();
  });

  it('should catch and log errors when closing the Spanner client', async () => {
    const { database, spanner } = await createInjectedService();
    jest.spyOn(spanner, 'close');
    jest
      .spyOn(database, 'close')
      .mockRejectedValue(new SessionLeakError(['🚰', '💦']));

    await testModule?.close();

    expect(getLoggedErrors()).toEqual([
      expect.objectContaining({
        message: 'Failed to close Spanner client.',
        error: expect.stringContaining('leak'),
        spannerLeaks: ['🚰', '💦'],
      }),
    ]);
  });

  async function createInjectedService(
    args: Parameters<(typeof SpannerModule)['forRoot']> = [],
  ): Promise<MyService> {
    testModule = await Test.createTestingModule({
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
