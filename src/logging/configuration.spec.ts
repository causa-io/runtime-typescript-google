import type { getLoggedObjects } from '@causa/runtime/testing';
import { jest } from '@jest/globals';
import { Logger } from 'pino';
import { googlePinoConfiguration } from './configuration.js';

describe('configuration', () => {
  let logger: Logger;
  let getLogs: typeof getLoggedObjects;

  beforeEach(async () => {
    jest.resetModules();
    const { getDefaultLogger, updatePinoConfiguration } = await import(
      '@causa/runtime'
    );
    const { spyOnLogger, getLoggedObjects } = await import(
      '@causa/runtime/testing'
    );
    getLogs = getLoggedObjects;
    updatePinoConfiguration({
      ...googlePinoConfiguration,
      level: 'debug',
    });
    spyOnLogger();
    logger = getDefaultLogger();
  });

  it('should add the @type field for errors', () => {
    logger.warn('⚠️');
    logger.error('❌');
    logger.error({ message: '🚨', extra: 1234 });
    logger.error({ extra: 1234 }, '%s', '💀');
    logger.error({ message: '%s', extra: 1234 }, '💥');
    logger.warn('%s %d', 'err', 4567);
    logger.error('%s %d', 'err', 4567);

    const actualLogs = getLogs();

    expect(actualLogs[0]).not.toHaveProperty('@type');
    expect(actualLogs[1]).toHaveProperty(
      '@type',
      'type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent',
    );
    expect(actualLogs[2]).toMatchObject({
      '@type':
        'type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent',
      message: '🚨',
      extra: 1234,
    });
    expect(actualLogs[3]).toMatchObject({
      '@type':
        'type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent',
      message: '💀',
      extra: 1234,
    });
    expect(actualLogs[4]).toMatchObject({
      '@type':
        'type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent',
      message: '💥',
      extra: 1234,
    });
    expect(actualLogs[5]).toMatchObject({
      message: 'err 4567',
    });
    expect(actualLogs[5]).not.toHaveProperty('@type');
    expect(actualLogs[6]).toMatchObject({
      '@type':
        'type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent',
      message: 'err 4567',
    });
  });

  it('should map the severity levels', () => {
    logger.debug('🐛');
    logger.info('📢');
    logger.warn('⚠️');
    logger.error('❌');

    const actualLogs = getLogs();

    expect(actualLogs[0]).toMatchObject({
      severity: 'DEBUG',
    });
    expect(actualLogs[1]).toMatchObject({
      severity: 'INFO',
    });
    expect(actualLogs[2]).toMatchObject({
      severity: 'WARNING',
    });
    expect(actualLogs[3]).toMatchObject({
      severity: 'ERROR',
    });
  });

  it('should redact the Firebase AppCheck header', () => {
    logger.info({ req: { headers: { 'x-firebase-appcheck': 'token' } } }, '🔒');

    const actualLogs = getLogs();

    expect(actualLogs[0]).toMatchObject({
      req: { headers: { 'x-firebase-appcheck': '[Redacted]' } },
    });
  });
});
