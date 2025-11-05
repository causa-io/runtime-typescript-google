import { Database } from '@google-cloud/spanner';
import 'jest-extended';
import { SpannerEntityManager } from './entity-manager.js';
import { InvalidEntityDefinitionError } from './errors.js';
import {
  clearAllTestEntities,
  setupTestDatabase,
  SomeEntity,
} from './entity-manager.test.js';

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

  describe('sqlTable', () => {
    it('should return the quoted table name for the entity type', () => {
      const actualTableName = manager.sqlTable(SomeEntity);

      expect(actualTableName).toEqual('`MyEntity`');
    });

    it('should return the quoted table name for a string', () => {
      const actualTableName = manager.sqlTable('MyTable');

      expect(actualTableName).toEqual('`MyTable`');
    });

    it('should add the index to the table name', () => {
      const actualTableName = manager.sqlTable('MyTable', {
        index: 'MyIndex',
      });

      expect(actualTableName).toEqual('`MyTable`@{FORCE_INDEX=`MyIndex`}');
    });

    it('should add the index and the Spanner emulator hint', () => {
      const actualTableName = manager.sqlTable('MyTable', {
        index: 'MyIndex',
        disableQueryNullFilteredIndexEmulatorCheck: true,
      });

      expect(actualTableName).toEqual(
        '`MyTable`@{FORCE_INDEX=`MyIndex`,spanner_emulator.disable_query_null_filtered_index_check=true}',
      );
    });

    it('should throw when the type is not a valid entity type', () => {
      expect(() => manager.sqlTable({} as any)).toThrow(
        InvalidEntityDefinitionError,
      );
    });
  });
});
