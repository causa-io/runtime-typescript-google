import { LogFn, Logger, LoggerOptions } from 'pino';

/**
 * Options that should be passed to `updatePinoConfiguration` when initializing a logger for a GCP environment.
 * Errors will be tagged with the Cloud Error Reporting-specific `@type` key, such that they are detected by the
 * service. Severity levels are also mapped to the ones used by Cloud Logging.
 */
export const googlePinoConfiguration: LoggerOptions = {
  hooks: {
    logMethod(args: any[], method: LogFn, level: number) {
      let binding: any;
      if (typeof args[0] === 'object') {
        binding = args[0];
      } else {
        binding = {};
        args.unshift(binding);
      }

      const levelMapping = (this as Logger).levels.values;
      if (level >= levelMapping.error) {
        binding['@type'] =
          'type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent';
      }

      // https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#LogSeverity
      binding.severity =
        {
          [levelMapping.debug]: 'DEBUG',
          [levelMapping.info]: 'INFO',
          [levelMapping.warn]: 'WARNING',
          [levelMapping.error]: 'ERROR',
        }[level] ?? 'CRITICAL';

      method.apply(this, args as any);
    },
  },
};
