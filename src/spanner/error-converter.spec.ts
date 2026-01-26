import { EntityAlreadyExistsError, RetryableError } from '@causa/runtime';
import { SessionPoolExhaustedError } from '@google-cloud/spanner/build/src/session-pool.js';
import { status } from '@grpc/grpc-js';
import { convertSpannerToEntityError } from './error-converter.js';
import {
  InvalidArgumentError,
  InvalidQueryError,
  TemporarySpannerError,
  UnexpectedSpannerError,
} from './errors.js';

describe('convertSpannerToEntityError', () => {
  it('should return undefined for an unknown error', () => {
    const error = new Error('💥');

    const actual = convertSpannerToEntityError(error);

    expect(actual).toBeUndefined();
  });

  it('should return undefined for an error with an unknown gRPC status code', () => {
    const error = Object.assign(new Error('💥'), { code: 999 });

    const actual = convertSpannerToEntityError(error);

    expect(actual).toBeUndefined();
  });

  it('should return undefined if it is already a RetryableError', () => {
    class CustomRetryableError extends RetryableError {}
    const error = new CustomRetryableError('🔄');

    const actual = convertSpannerToEntityError(error);

    expect(actual).toBeUndefined();
  });

  it('should return undefined if it is already an UnexpectedSpannerError', () => {
    class CustomUnexpectedError extends UnexpectedSpannerError {}
    const error = new CustomUnexpectedError('🤷');

    const actual = convertSpannerToEntityError(error);

    expect(actual).toBeUndefined();
  });

  it('should convert a SessionPoolExhaustedError to a TemporarySpannerError', () => {
    const error = new SessionPoolExhaustedError([]);

    const actual = convertSpannerToEntityError(error);

    expect(actual).toBeInstanceOf(TemporarySpannerError);
  });

  it('should convert a session acquisition timeout error to a TemporarySpannerError', () => {
    const error = new Error('Timeout occurred while acquiring session.');

    const actual = convertSpannerToEntityError(error);

    expect(actual).toBeInstanceOf(TemporarySpannerError);
  });

  it('should convert an INVALID_ARGUMENT error to an InvalidArgumentError', () => {
    const error = Object.assign(new Error('🤷'), {
      code: status.INVALID_ARGUMENT,
    });

    const actual = convertSpannerToEntityError(error);

    expect(actual).toBeInstanceOf(InvalidArgumentError);
    expect(actual?.message).toBe('🤷');
  });

  it('should convert a NOT_FOUND error to an InvalidQueryError', () => {
    const error = Object.assign(new Error('🙈'), { code: status.NOT_FOUND });

    const actual = convertSpannerToEntityError(error);

    expect(actual).toBeInstanceOf(InvalidQueryError);
  });

  it('should convert an ALREADY_EXISTS error to an EntityAlreadyExistsError', () => {
    const error = Object.assign(new Error('👬'), {
      code: status.ALREADY_EXISTS,
    });

    const actual = convertSpannerToEntityError(error);

    expect(actual).toBeInstanceOf(EntityAlreadyExistsError);
  });

  it.each([
    status.CANCELLED,
    status.DEADLINE_EXCEEDED,
    status.INTERNAL,
    status.UNAVAILABLE,
    status.ABORTED,
    status.RESOURCE_EXHAUSTED,
  ])(
    'should convert a gRPC error with code %s to a TemporarySpannerError with cause',
    (code) => {
      const error = Object.assign(new Error('⌛'), { code });

      const actual = convertSpannerToEntityError(error);

      expect(actual).toBeInstanceOf(TemporarySpannerError);
      expect((actual as TemporarySpannerError).code).toBe(code);
      expect(actual?.cause).toBe(error);
    },
  );
});
