import { Database } from '@google-cloud/spanner';
import 'jest-extended';
import { SpannerColumn } from './column.decorator.js';
import { SpannerEntityManager } from './entity-manager.js';
import { InvalidEntityDefinitionError } from './errors.js';
import { SpannerTable } from './table.decorator.js';
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

  describe('sqlColumns', () => {
    it('should return the quoted column names for the entity type', () => {
      const actualColumns = manager.sqlColumns(SomeEntity);

      expect(actualColumns).toEqual('`id`, `value`');
    });

    it('should return the quoted column names for a list of columns', () => {
      const actualColumns = manager.sqlColumns(['id', 'value']);

      expect(actualColumns).toEqual('`id`, `value`');
    });

    it('should throw when the type is not a valid entity type', () => {
      expect(() => manager.sqlColumns({} as any)).toThrow(
        InvalidEntityDefinitionError,
      );
    });

    it('should only return the columns for the specified properties', () => {
      @SpannerTable({ primaryKey: ['id'] })
      class EntityWithCustomName {
        @SpannerColumn()
        id!: string;

        @SpannerColumn({ name: 'customName' })
        value!: string;
      }

      const actualColumns = manager.sqlColumns(EntityWithCustomName, {
        forProperties: ['value'],
      });

      expect(actualColumns).toEqual('`customName`');
    });

    it.each([SomeEntity, ['id', 'value']])(
      'should prefix with the alias',
      (cols) => {
        const actualColumns = manager.sqlColumns(cols, { alias: 't' });

        expect(actualColumns).toEqual('`t`.`id`, `t`.`value`');
      },
    );
  });
});
