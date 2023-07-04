import { getDefaultLogger } from '@causa/runtime';
import { Database, Spanner } from '@google-cloud/spanner';
import { SessionPoolOptions } from '@google-cloud/spanner/build/src/session-pool.js';
import { Logger } from 'pino';

/**
 * The default pool options when instantiating a database object for use within a Cloud Function.
 */
export const SPANNER_SESSION_POOL_OPTIONS_FOR_CLOUD_FUNCTIONS: SessionPoolOptions =
  {
    // Default is `100`, which is really high for Cloud Functions that are supposed to handle a single request at a time.
    // This might change with Cloud Functions v2 that can handle concurrent requests.
    max: 20,
    // Again, this is lower than the default (`25`) for the same reason as above.
    // Also, this "overrides" `maxIdles`, hence the number of idle sessions will be 2, not 1.
    min: 2,
    // Default is `25`. This mirrors the defaults by having the same value as `min`.
    incStep: 2,

    // The following parameters come from the GitHub issue about unhandled timeout rejections:
    // https://github.com/googleapis/nodejs-spanner/issues/1682#issuecomment-1225187624

    // Setting this to a high value means actually idle sessions will be kept around for longer. As those sessions are not
    // kept alive (pinged), there's a chance they will not be (re)usable if the Cloud Function stays idle for too long.
    // However:
    // - Leaving a low value (tens of minutes) will cause the sessions to be proactively deleted between Cloud Function
    //   executions (when the CF is idle for a few minutes), which will fail because network is disabled at those times.
    //   This will generate more (harmless) errors.
    // - With a high value, even though the session will not be properly deleted, there is little chance the sessions will
    //   be reused after being idle, simply because the Cloud Function instance itself will probably be deleted if it is
    //   inactive.
    // Therefore setting a high value should not impact performances and limit the amount of noisy errors.
    idlesAfter: 30000,

    // This will practically disable the keep-alive calls for sessions.
    // When running in Cloud Functions, network calls (and CPU) are disabled when the Cloud Function is idle. This means
    // there is a high chance the keep alive mechanism won't work anyway.
    // 30000 minutes is still below the max signed 32-bit integer once converted to milliseconds, contrary to the
    // suggested value of 500000. If this constraint is not satisfied, `setTimeout`/`setInterval` complains and falls back
    // to 1 milliseconds.
    keepAlive: 30000,

    acquireTimeout: 30000,
    concurrency: 100,
    fail: true,
  };

/**
 * The default pool options when instantiating a database object for use within a service.
 * A "service" here is for example a Docker container that always has network access, contrary to Cloud Functions for
 * which internet traffic is disabled between invocations.
 */
export const SPANNER_SESSION_POOL_OPTIONS_FOR_SERVICE: SessionPoolOptions = {
  min: 5,
  incStep: 5, // Default is `25`. This mirrors the defaults by having the same value as `min`.
  acquireTimeout: 30000,
  concurrency: 100,
  fail: true,
};

/**
 * The "default Spanner database" singleton.
 */
let database!: Database;

/**
 * Instantiates a database object using the configuration found in the environment variables and the (`gbase`) default
 * session pool options.
 *
 * @param options Session pool options when instantiating the database. Default is {@link SPANNER_SESSION_POOL_OPTIONS_FOR_CLOUD_FUNCTIONS}.
 * @returns The database.
 */
export function getDefaultSpannerDatabaseForCloudFunction(): Database {
  if (database) {
    return database;
  }

  const instanceName = process.env.SPANNER_INSTANCE ?? '';
  const databaseName = process.env.SPANNER_DATABASE ?? '';

  database = new Spanner()
    .instance(instanceName)
    .database(databaseName, SPANNER_SESSION_POOL_OPTIONS_FOR_CLOUD_FUNCTIONS);

  catchSpannerDatabaseErrors(database, getDefaultLogger());

  return database;
}

/**
 * Catches errors emitted from a database object, logging them as errors using the given logger.
 *
 * @param database The database for which errors should be caught.
 * @param logger The logger used to log database errors.
 */
export function catchSpannerDatabaseErrors(database: Database, logger: Logger) {
  database.on('error', (error: Error) => {
    logger.info(
      { error: error.stack },
      'Uncaught Spanner database error. This might be due to the background keep alive mechanism running in an idle Cloud Function.',
    );
  });
}
