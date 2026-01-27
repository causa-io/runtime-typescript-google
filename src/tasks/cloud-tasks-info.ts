import { AllowMissing } from '@causa/runtime';
import { Expose, Transform } from 'class-transformer';
import { IsDate, IsInt, IsString, Min } from 'class-validator';

/**
 * Information about a Cloud Tasks task, extracted from HTTP headers.
 */
export class CloudTasksInfo {
  /**
   * The name of the queue.
   */
  @Expose({ name: 'x-cloudtasks-queuename' })
  @IsString()
  readonly queueName!: string;

  /**
   * The short name of the task, or a unique system-generated ID if no name was specified at creation.
   */
  @Expose({ name: 'x-cloudtasks-taskname' })
  @IsString()
  readonly taskName!: string;

  /**
   * The number of times this task has been retried. For the first attempt, this value is `0`.
   */
  @Expose({ name: 'x-cloudtasks-taskretrycount' })
  @Transform(({ value }) => parseInt(value))
  @IsInt()
  @Min(0)
  readonly retryCount!: number;

  /**
   * The total number of times that the task has received a response from the handler.
   */
  @Expose({ name: 'x-cloudtasks-taskexecutioncount' })
  @Transform(({ value }) => parseInt(value))
  @IsInt()
  @Min(0)
  readonly executionCount!: number;

  /**
   * The schedule time of the task.
   */
  @Expose({ name: 'x-cloudtasks-tasketa' })
  @Transform(({ value }) => new Date(parseFloat(value) * 1000))
  @IsDate()
  readonly eta!: Date;

  /**
   * The HTTP response code from the previous retry.
   * Only present if this is a retry attempt.
   */
  @Expose({ name: 'x-cloudtasks-taskpreviousresponse' })
  @Transform(({ value }) => (value !== undefined ? parseInt(value) : undefined))
  @IsInt()
  @AllowMissing()
  readonly previousResponse?: number;

  /**
   * The reason for retrying the task.
   * Only present if this is a retry attempt.
   */
  @Expose({ name: 'x-cloudtasks-taskretryreason' })
  @IsString()
  @AllowMissing()
  readonly retryReason?: string;
}
