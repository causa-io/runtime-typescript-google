import { Database, Transaction } from '@google-cloud/spanner';
import { status } from '@grpc/grpc-js';
import { jest } from '@jest/globals';
import 'jest-extended';
import { Readable } from 'node:stream';
import { SpannerEntityManager } from './entity-manager.js';
import {
  clearAllTestEntities,
  IntEntity,
  setupTestDatabase,
  SomeEntity,
} from './entity-manager.test.js';
import { InvalidArgumentError, TemporarySpannerError } from './errors.js';
import { SpannerRequestPriority } from './types.js';

describe('SpannerEntityManager', () => {
  let database: Database;
  let manager: SpannerEntityManager;

  beforeAll(async () => {
    ({ database } = await setupTestDatabase());
  });

  beforeEach(() => {
    manager = new SpannerEntityManager(database);
  });

  afterEach(() => clearAllTestEntities(manager));

  afterAll(() => database.delete());

  describe('queryStream', () => {
    it('should run the query and return an async iterable of typed entities', async () => {
      await database.table('MyEntity').insert([
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
      ]);
      jest.spyOn(manager.database, 'runStream');

      const actualEntities: SomeEntity[] = [];
      for await (const entity of manager.queryStream(
        { entityType: SomeEntity },
        { sql: 'SELECT id, value FROM MyEntity ORDER BY id' },
      )) {
        actualEntities.push(entity);
      }

      expect(actualEntities).toEqual([
        { id: '1', value: '🎁' },
        { id: '2', value: '🎉' },
      ]);
      expect(actualEntities[0]).toBeInstanceOf(SomeEntity);
      expect(actualEntities[1]).toBeInstanceOf(SomeEntity);
      // `Database.runStream` should not be called because it breaks async iterables.
      // A transaction (snapshot) should always be used instead.
      expect(manager.database.runStream).not.toHaveBeenCalled();
    });

    it('should run the query without typed entities', async () => {
      await database.table('IntEntity').insert([{ id: '1', value: 10 }]);

      const actualEntities: any[] = [];
      for await (const entity of manager.queryStream({
        sql: 'SELECT id, value FROM IntEntity',
      })) {
        actualEntities.push(entity);
      }

      expect(actualEntities).toEqual([{ id: '1', value: 10 }]);
      expect(actualEntities[0]).not.toBeInstanceOf(IntEntity);
    });

    it('should use a read only transaction by default', async () => {
      const stream = manager.queryStream({
        sql: `INSERT INTO IntEntity(id, value) VALUES ('1', 10)`,
      });

      const actualPromise = (async () => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _ of stream) {
        }
      })();

      await expect(actualPromise).rejects.toThrow(InvalidArgumentError);
    });

    it('should use the provided transaction', async () => {
      await manager.transaction(async (transaction) => {
        const stream = manager.queryStream(
          { transaction },
          { sql: `INSERT INTO IntEntity(id, value) VALUES ('1', 10)` },
        );

        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _ of stream) {
        }
      });

      const [actualRows] = await database.table('IntEntity').read({
        keys: ['1'],
        columns: ['id', 'value'],
        json: true,
      });
      expect(actualRows).toEqual([{ id: '1', value: 10 }]);
    });

    it('should pass the request options to the query', async () => {
      let spy!: jest.SpiedFunction<Transaction['runStream']>;
      const requestOptions = { priority: SpannerRequestPriority.PRIORITY_LOW };

      await manager.snapshot(async (transaction) => {
        spy = jest.spyOn(transaction, 'runStream');
        const stream = manager.queryStream(
          { transaction, requestOptions },
          { sql: 'SELECT 1' },
        );

        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _ of stream) {
        }
      });

      expect(spy).toHaveBeenCalledWith(
        expect.objectContaining({ requestOptions }),
      );
    });

    it('should filter out undefined rows sent by the PartialResultStream', async () => {
      const actualRows = await manager.snapshot(async (transaction) => {
        jest
          .spyOn(transaction, 'runStream')
          .mockReturnValueOnce(
            Readable.from([{ value: '🎁' }, undefined, { value: '🎉' }]) as any,
          );

        const stream = manager.queryStream(
          { transaction },
          { sql: 'SELECT value FROM MyEntity ORDER BY id' },
        );

        const rows: any[] = [];
        for await (const row of stream) {
          rows.push(row);
        }
        return rows;
      });

      expect(actualRows).toEqual([{ value: '🎁' }, { value: '🎉' }]);
    });

    it('should convert Spanner errors when getSnapshot fails', async () => {
      const error = new Error('💥') as any;
      error.code = status.UNAVAILABLE;
      jest
        .spyOn(manager.database, 'getSnapshot')
        .mockRejectedValueOnce(error as never);

      const actualPromise = (async () => {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _ of manager.queryStream({ sql: 'SELECT 1' })) {
        }
      })();

      await expect(actualPromise).rejects.toThrow(TemporarySpannerError);
    });
  });
});
