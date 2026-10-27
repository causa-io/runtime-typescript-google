import { levels, type LoggerOptions } from 'pino';

/**
 * The type added to error logs, such that they are detected by Cloud Error Reporting.
 */
const ERROR_REPORTING_TYPE =
  'type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent';

/**
 * The Cloud Logging severities for each pino level label. Other levels are mapped to `CRITICAL`.
 * https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#LogSeverity
 */
const SEVERITIES: Record<string, string> = {
  trace: 'DEBUG',
  debug: 'DEBUG',
  info: 'INFO',
  warn: 'WARNING',
  error: 'ERROR',
};

/**
 * Options that should be passed to `updatePinoConfiguration` when initializing a logger for a GCP environment.
 * Errors will be tagged with the Cloud Error Reporting-specific `@type` key, such that they are detected by the
 * service. Severity levels are also mapped to the ones used by Cloud Logging.
 * The `X-Firebase-AppCheck` header is redacted from logs.
 */
export const googlePinoConfiguration: LoggerOptions = {
  redact: {
    paths: ['req.headers["x-firebase-appcheck"]'],
  },
  formatters: {
    level(label, level) {
      const severity = SEVERITIES[label] ?? 'CRITICAL';
      return level >= levels.values.error
        ? { level, severity, '@type': ERROR_REPORTING_TYPE }
        : { level, severity };
    },
  },
};
