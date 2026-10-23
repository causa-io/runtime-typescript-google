/**
 * Information about a Cloud Tasks task, extracted from HTTP headers.
 */
export class CloudTasksInfo {
  constructor(init: CloudTasksInfo) {
    Object.assign(this, init);
  }

  /**
   * The name of the queue.
   * This is the `X-CloudTasks-QueueName` header.
   */
  readonly queueName!: string;

  /**
   * The short name of the task, or a unique system-generated ID if no name was specified at creation.
   * This is the `X-CloudTasks-TaskName` header.
   */
  readonly taskName!: string;

  /**
   * The number of times this task has been retried. For the first attempt, this value is `0`.
   * This is the `X-CloudTasks-TaskRetryCount` header.
   */
  readonly retryCount!: number;

  /**
   * The total number of times that the task has received a response from the handler.
   * This is the `X-CloudTasks-TaskExecutionCount` header.
   */
  readonly executionCount!: number;

  /**
   * The schedule time of the task.
   * This is the `X-CloudTasks-TaskETA` header.
   */
  readonly eta!: Date;

  /**
   * The HTTP response code from the previous retry.
   * Only present if this is a retry attempt.
   * This is the `X-CloudTasks-TaskPreviousResponse` header.
   */
  readonly previousResponse?: number;

  /**
   * The reason for retrying the task.
   * Only present if this is a retry attempt.
   * This is the `X-CloudTasks-TaskRetryReason` header.
   */
  readonly retryReason?: string;
}
