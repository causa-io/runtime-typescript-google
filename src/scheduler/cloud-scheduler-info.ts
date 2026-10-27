/**
 * Information about a Cloud Scheduler job, extracted from HTTP headers.
 */
export class CloudSchedulerInfo {
  constructor(init: CloudSchedulerInfo) {
    Object.assign(this, init);
  }

  /**
   * The name of the Cloud Scheduler job.
   * This is the `X-CloudScheduler-JobName` header.
   */
  readonly jobName!: string;

  /**
   * The scheduled time for the job execution.
   * This is the `X-CloudScheduler-ScheduleTime` header.
   */
  readonly scheduleTime?: Date;
}
