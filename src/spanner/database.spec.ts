import { getLoggedInfos, spyOnLogger } from '@causa/runtime/testing';
import { Database } from '@google-cloud/spanner';
import { getDefaultSpannerDatabaseForCloudFunction } from './database.js';
import { createDatabase } from './testing.js';

describe('database', () => {
  let database: Database;
  let previousEnv: NodeJS.ProcessEnv;

  beforeAll(async () => {
    database = await createDatabase();
    previousEnv = process.env;
    process.env.SPANNER_DATABASE = database.formattedName_.split('/').at(-1);

    spyOnLogger();
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

  it('should catch emitted errors', async () => {
    const db = getDefaultSpannerDatabaseForCloudFunction();

    db.emit('error', new Error('💥'));

    expect(getLoggedInfos()).toEqual([
      expect.objectContaining({
        message:
          'Uncaught Spanner database error. This might be due to the background keep alive mechanism running in an idle Cloud Function.',
        error: expect.stringContaining('💥'),
      }),
    ]);
  });
});
