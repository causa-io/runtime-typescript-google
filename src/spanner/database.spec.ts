import { Database } from '@google-cloud/spanner';
import { getDefaultSpannerDatabaseForCloudFunction } from './database.js';
import { createDatabase } from './testing.js';

describe('database', () => {
  let database: Database;
  let previousEnv: NodeJS.ProcessEnv;

  beforeAll(async () => {
    database = await createDatabase();
    previousEnv = { ...process.env };
    process.env.SPANNER_DATABASE = database.formattedName_.split('/').at(-1);
  });

  afterAll(async () => {
    await database.getMetadata(); // Avoids errors when deleting the database too quickly.
    await database.delete();
    process.env = previousEnv;
  });

  it('should return the singleton default database', () => {
    const db1 = getDefaultSpannerDatabaseForCloudFunction();
    const db2 = getDefaultSpannerDatabaseForCloudFunction();

    // The first two components are `projects/<projectId>`.
    // However `db1` still uses a placeholder (`{{projectId}}`), while `database` has the actual (demo) project ID.
    expect(db1.formattedName_.split('/').slice(2)).toEqual(
      database.formattedName_.split('/').slice(2),
    );
    expect(db1).toBe(db2);
  });
});
