import { Database, Instance, Spanner } from '@google-cloud/spanner';
import 'jest-extended';
import { createDatabase } from './testing.js';

describe('createDatabase', () => {
  let previousEnv: NodeJS.ProcessEnv;
  let spanner: Spanner;
  let instance: Instance;

  beforeAll(async () => {
    spanner = new Spanner();
    const [newInstance, operation] = await spanner.createInstance(
      'test-instance',
      { displayName: 'test-utils', config: 'test/local' },
    );
    await operation.promise();
    instance = newInstance;
  });

  beforeEach(() => {
    previousEnv = { ...process.env };
    delete process.env.SPANNER_DATABASE;
  });

  afterEach(() => {
    process.env = previousEnv;
  });

  afterAll(async () => {
    await instance.delete();
    spanner.close();
  });

  it('should use the default instance', async () => {
    process.env.SPANNER_INSTANCE = 'test-instance';

    const actualDatabase = await createDatabase();

    expect(actualDatabase.formattedName_).toContain(
      `instances/test-instance/databases/`,
    );
  });

  it('should use the provided instance', async () => {
    const actualDatabase = await createDatabase({
      name: 'test-db',
      instance,
    });

    expect(actualDatabase.formattedName_).toEndWith(
      'instances/test-instance/databases/test-db',
    );
  });

  it('should use the provided Spanner client', async () => {
    const actualDatabase = await createDatabase({
      name: 'test-db',
      spanner,
    });

    const actualInstance = (actualDatabase as any).parent;
    const actualSpanner = (actualInstance as any).parent;
    expect(actualSpanner).toBe(spanner);
  });

  it('should create a database that does not exist', async () => {
    const database = await createDatabase({
      name: 'test-create-database',
      instance,
    });
    await database.close();

    let actualDatabase: Database | undefined;
    let actualDatabaseExists!: boolean;
    try {
      actualDatabase = instance.database('test-create-database');
      [actualDatabaseExists] = await actualDatabase.exists();
    } finally {
      await actualDatabase?.delete();
      await actualDatabase?.close();
    }
    expect(actualDatabaseExists).toBeTrue();
  });

  it('should recreate a database that exists', async () => {
    const existingDatabase = await createDatabase({
      name: 'test-create-database',
      instance,
    });
    await existingDatabase.createTable(
      'CREATE TABLE MyTable (id STRING(64) NOT NULL) PRIMARY KEY (id)',
    );
    await existingDatabase.close();

    const database = await createDatabase({
      name: 'test-create-database',
      instance,
    });
    await database?.close();

    let actualDatabase: Database | undefined;
    let actualSchema!: string[];
    try {
      actualDatabase = instance.database('test-create-database');
      [actualSchema] = await actualDatabase.getSchema();
    } finally {
      await actualDatabase?.delete();
      await actualDatabase?.close();
    }
    expect(actualSchema).toEqual([]);
  });

  it('should copy the DDL from the provided source database', async () => {
    let existingDatabase!: Database;
    let actualDatabase!: Database;

    try {
      existingDatabase = await createDatabase({
        name: 'test-create-database-existing',
        instance,
      });
      await existingDatabase.createTable(
        'CREATE TABLE MyTable (id STRING(64) NOT NULL,) PRIMARY KEY (id)',
      );
      const [expectedDdl] = await existingDatabase.getSchema();

      const database = await createDatabase({
        name: 'test-create-database',
        sourceDatabaseName: 'test-create-database-existing',
        instance,
      });
      await database?.close();

      actualDatabase = instance.database('test-create-database');

      const [actualSchema] = await actualDatabase.getSchema();
      expect(actualSchema).toEqual(expectedDdl);
    } finally {
      await existingDatabase?.delete();
      await actualDatabase?.delete();
    }
  });

  it('should copy the DDL from the SPANNER_DATABASE', async () => {
    let existingDatabase!: Database;
    let actualDatabase!: Database;

    try {
      existingDatabase = await createDatabase({
        instance,
        name: 'test-create-database-existing',
      });
      await existingDatabase.createTable(
        'CREATE TABLE MyTable (id STRING(64) NOT NULL,) PRIMARY KEY (id)',
      );
      const [expectedDdl] = await existingDatabase.getSchema();

      process.env.SPANNER_DATABASE = 'test-create-database-existing';

      const database = await createDatabase({
        name: 'test-create-database',
        instance,
      });
      await database.close();

      actualDatabase = instance.database('test-create-database');

      const [actualSchema] = await actualDatabase.getSchema();
      expect(actualSchema).toEqual(expectedDdl);
    } finally {
      await existingDatabase.delete();
      await actualDatabase.delete();
    }
  });
});
