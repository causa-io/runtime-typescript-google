import { CloudTasksClient, protos } from '@google-cloud/tasks';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RetryOptions, grpc } from 'google-gax';
import { TemporaryCloudTasksError } from './errors.js';

export type Task = protos.google.cloud.tasks.v2.ITask;
export const HttpMethod = protos.google.cloud.tasks.v2.HttpMethod;
export type HttpRequest = protos.google.cloud.tasks.v2.IHttpRequest;

/**
 * Defines the HTTP request within a Cloud Tasks task.
 */
export type HttpRequestCreation = Omit<HttpRequest, 'body' | 'oidcToken'> & {
  /**
   * The body of the HTTP request. If it is an object, it will be serialized to JSON.
   * The `Content-Type` header will be set to `application/json`.
   */
  body?: HttpRequest['body'] | object;

  /**
   * The OIDC token to use for the request. If set to `'self'`, the currently used service account will be used.
   */
  oidcToken?: HttpRequest['oidcToken'] | 'self';
};

/**
 * The default retry options when creating a Cloud Tasks using {@link scheduleCloudFunctionTask}.
 * The `createTask` method of the client does not retry creations by default. The parameters below are the default
 * parameters for idempotent operations in the `cloud_tasks_client_config.json` configuration file.
 */
const RETRY_CONFIG: RetryOptions = {
  retryCodes: [grpc.status.DEADLINE_EXCEEDED, grpc.status.UNAVAILABLE],
  backoffSettings: {
    initialRetryDelayMillis: 100,
    retryDelayMultiplier: 1.3,
    maxRetryDelayMillis: 10000,
    initialRpcTimeoutMillis: 20000,
    rpcTimeoutMultiplier: 1,
    maxRpcTimeoutMillis: 20000,
    totalTimeoutMillis: 600000,
  },
};

/**
 * The list of gRPC status codes that should be retried when creating a Cloud Tasks task.
 * Errors could come from the Cloud Tasks request, or from the authentication request to get the service account email.
 */
const GRPC_RETRYABLE_CODES = [
  grpc.status.CANCELLED,
  grpc.status.DEADLINE_EXCEEDED,
  grpc.status.INTERNAL,
  grpc.status.UNAVAILABLE,
];

/**
 * A service that can be used to schedule Cloud Tasks using a {@link CloudTasksClient}.
 */
@Injectable()
export class CloudTasksScheduler {
  constructor(
    private readonly client: CloudTasksClient,
    private readonly configService: ConfigService,
  ) {}

  /**
   * Returns the full path of the queue with the given name.
   * It gets the path from the configuration, using the `TASKS_QUEUE_` prefix and the uppercase and underscored name.
   *
   * @param name The name of the queue.
   * @returns The full path of the queue, or `null` if the configuration could not be found.
   */
  getQueuePath(name: string): string | null {
    const configName = `TASKS_QUEUE_${name.toUpperCase().replace(/[\.-]{1}/g, '_')}`;
    const queuePath = this.configService.get<string>(configName);
    return queuePath ?? null;
  }

  /**
   * The promise resolving to the email of the service account used by the Cloud Tasks client.
   * This assumes that it is the account used by the current service / application.
   */
  private selfServiceAccountEmailPromise: Promise<string> | undefined;

  /**
   * Returns the email of the service account used by the current service / application, which can be used to perform
   * HTTP requests in Cloud Tasks tasks as the same identity as the creator of the task.
   *
   * @returns The email of the service account used by the current service / application.
   */
  private getSelfServiceAccountEmail(): Promise<string> {
    if (!this.selfServiceAccountEmailPromise) {
      this.selfServiceAccountEmailPromise = this.client.auth
        .getCredentials()
        .then((credentials) => credentials.client_email ?? '');
    }

    return this.selfServiceAccountEmailPromise;
  }

  /**
   * Transforms the {@link HttpRequestCreation} into a {@link HttpRequest} that can be passed to Cloud Tasks.
   * This handles the optional serialization of the body and the `oidcToken` property (when it should reference the
   * current service account).
   *
   * @param httpRequest The {@link HttpRequestCreation} passed by the caller.
   * @returns An {@link HttpRequest} that can be passed to Cloud Tasks.
   */
  private async makeHttpRequest(
    httpRequest: HttpRequestCreation,
  ): Promise<HttpRequest> {
    if (typeof httpRequest.body === 'object') {
      const jsonString = JSON.stringify(httpRequest.body);
      httpRequest.body = Buffer.from(jsonString).toString('base64');
      httpRequest.headers = {
        ...httpRequest.headers,
        'Content-Type': 'application/json',
      };
    }

    if (httpRequest.oidcToken === 'self') {
      const serviceAccountEmail = await this.getSelfServiceAccountEmail();
      httpRequest.oidcToken = { serviceAccountEmail };
    }

    return httpRequest as HttpRequest;
  }

  /**
   * Creates a Cloud Tasks task that will invoke an HTTP endpoint.
   *
   * @param queue The Cloud Tasks queue to schedule the task in.
   * @param scheduleDate The date at which the task should be scheduled.
   * @param httpRequest The HTTP request to perform by the task.
   * @param options Options when creating the task.
   * @returns The created task.
   */
  async schedule(
    queue: string,
    scheduleDate: Date,
    creation: HttpRequestCreation,
    options: {
      /**
       * Whether to retry the creation of the task if it fails.
       * If set to `true` (the default), the default retry configuration will be used.
       */
      retry?: boolean | RetryOptions;

      /**
       * The name of the task. If not set, a random name will be generated by the server.
       */
      taskName?: string;
    } = {},
  ): Promise<Task> {
    try {
      const httpRequest = await this.makeHttpRequest(creation);

      const retry = options.retry ?? true;
      const scheduleTime = scheduleDate.getTime();
      const seconds = Math.floor(scheduleTime / 1000);
      const nanos = (scheduleTime - seconds * 1000) * 1e6;

      const name = options.taskName
        ? `${queue}/tasks/${options.taskName}`
        : undefined;

      const [task] = await this.client.createTask(
        {
          parent: queue,
          task: { name, httpRequest, scheduleTime: { seconds, nanos } },
        },
        retry ? { retry: retry === true ? RETRY_CONFIG : retry } : {},
      );

      return task;
    } catch (error: any) {
      if (GRPC_RETRYABLE_CODES.includes(error.code)) {
        throw new TemporaryCloudTasksError(error.message);
      }

      throw error;
    }
  }
}
