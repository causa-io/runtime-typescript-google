import { RetryableError } from '@causa/runtime';
import { CloudTasksClient } from '@google-cloud/tasks';
import { status } from '@grpc/grpc-js';
import { jest } from '@jest/globals';
import 'jest-extended';
import { CloudTasksScheduler, HttpMethod } from './scheduler.js';

const EXPECTED_TASK = { name: '📋' };
const EXPECTED_SERVICE_ACCOUNT_EMAIL = 'eixample@heetch.com';

describe('CloudTasksScheduler', () => {
  let client: CloudTasksClient;
  let scheduler: CloudTasksScheduler;
  let createTasksSpy: jest.SpiedFunction<any>;
  let getCredentialsSpy: jest.SpiedFunction<any>;

  beforeEach(() => {
    client = new CloudTasksClient();
    createTasksSpy = jest
      .spyOn(client as any, 'createTask')
      .mockResolvedValue([EXPECTED_TASK]);
    getCredentialsSpy = jest
      .spyOn(client.auth as any, 'getCredentials')
      .mockResolvedValue({ client_email: EXPECTED_SERVICE_ACCOUNT_EMAIL });
    scheduler = new CloudTasksScheduler(client);
  });

  afterEach(async () => {
    await client.close();
  });

  describe('schedule', () => {
    it('should create a regular task', async () => {
      const scheduleDate = new Date();

      const actualTask = await scheduler.schedule('MY_QUEUE', scheduleDate, {
        body: '☑️',
        httpMethod: HttpMethod.POST,
        url: 'https://example.com',
      });

      expect(actualTask).toEqual(EXPECTED_TASK);
      expect(client.createTask).toHaveBeenCalledWith(
        {
          parent: 'MY_QUEUE',
          task: {
            scheduleTime: {
              seconds: Math.floor(scheduleDate.getTime() / 1000),
              nanos: (scheduleDate.getTime() % 1000) * 1e6,
            },
            httpRequest: {
              httpMethod: HttpMethod.POST,
              url: 'https://example.com',
              body: '☑️',
            },
          },
        },
        { retry: expect.any(Object) },
      );
      expect(client.auth.getCredentials).not.toHaveBeenCalled();
    });

    it('should create a task with options', async () => {
      const scheduleDate = new Date();

      const actualTask = await scheduler.schedule(
        'MY_QUEUE',
        scheduleDate,
        {
          body: { doThis: '🤸' },
          httpMethod: HttpMethod.POST,
          headers: {
            'X-My-Header': '💡',
          },
          url: 'https://example.com',
          oidcToken: 'self',
        },
        {
          retry: false,
          taskName: 'MY_TASK',
        },
      );

      expect(actualTask).toEqual(EXPECTED_TASK);
      expect(client.createTask).toHaveBeenCalledWith(
        {
          parent: 'MY_QUEUE',
          task: {
            name: `MY_QUEUE/tasks/MY_TASK`,
            scheduleTime: {
              seconds: Math.floor(scheduleDate.getTime() / 1000),
              nanos: (scheduleDate.getTime() % 1000) * 1e6,
            },
            httpRequest: {
              httpMethod: HttpMethod.POST,
              url: 'https://example.com',
              body: expect.toSatisfy((body: Buffer) => {
                expect(
                  Buffer.from(body.toString(), 'base64').toString(),
                ).toEqual('{"doThis":"🤸"}');
                return true;
              }),
              headers: {
                'X-My-Header': '💡',
                'Content-Type': 'application/json',
              },
              oidcToken: {
                serviceAccountEmail: EXPECTED_SERVICE_ACCOUNT_EMAIL,
              },
            },
          },
        },
        {},
      );
      expect(client.auth.getCredentials).toHaveBeenCalledExactlyOnceWith();
    });

    it('should rethrow Cloud Tasks transient errors as retryable errors', async () => {
      const deadlineExceeded = new Error('🕰️');
      (deadlineExceeded as any).code = status.DEADLINE_EXCEEDED;
      createTasksSpy.mockRejectedValueOnce(deadlineExceeded);

      const actualPromise = scheduler.schedule('MY_QUEUE', new Date(), {});

      await expect(actualPromise).rejects.toThrow(RetryableError);
      await expect(actualPromise).rejects.toThrow('🕰️');
    });

    it('should rethrow credential transient errors as retryable errors', async () => {
      const internal = new Error('🔧');
      (internal as any).code = status.INTERNAL;
      getCredentialsSpy.mockRejectedValueOnce(internal);

      const actualPromise2 = scheduler.schedule('MY_QUEUE', new Date(), {
        oidcToken: 'self',
      });

      await expect(actualPromise2).rejects.toThrow(RetryableError);
      await expect(actualPromise2).rejects.toThrow('🔧');
    });
  });
});
