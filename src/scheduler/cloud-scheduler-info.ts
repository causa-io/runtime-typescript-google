import { AllowMissing, IsDateType } from '@causa/runtime';
import { Expose } from 'class-transformer';
import { IsString } from 'class-validator';

/**
 * Information about a Cloud Scheduler job, extracted from HTTP headers.
 */
export class CloudSchedulerInfo {
  /**
   * The name of the Cloud Scheduler job.
   */
  @Expose({ name: 'x-cloudscheduler-jobname' })
  @IsString()
  readonly jobName!: string;

  /**
   * The scheduled time for the job execution.
   */
  @Expose({ name: 'x-cloudscheduler-scheduletime' })
  @IsDateType()
  @AllowMissing()
  readonly scheduleTime?: Date;
}
