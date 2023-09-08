import { CloudTasksClient } from '@google-cloud/tasks';
import { jest } from '@jest/globals';
import 'jest-extended';
import { CloudTasksScheduler, HttpMethod } from './scheduler.js';

const EXPECTED_TASK = { name: '📋' };
const EXPECTED_SERVICE_ACCOUNT_EMAIL = 'eixample@heetch.com';

describe('CloudTasksScheduler', () => {
  let client: CloudTasksClient;
  let scheduler: CloudTasksScheduler;

  beforeEach(() => {
    client = new CloudTasksClient();
    jest.spyOn(client as any, 'createTask').mockResolvedValue([EXPECTED_TASK]);
    jest
      .spyOn(client.auth as any, 'getCredentials')
      .mockResolvedValue({ client_email: EXPECTED_SERVICE_ACCOUNT_EMAIL });
    scheduler = new CloudTasksScheduler(client);
  });

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
              expect(Buffer.from(body.toString(), 'base64').toString()).toEqual(
                '{"doThis":"🤸"}',
              );
              return true;
            }),
            headers: {
              'X-My-Header': '💡',
              'Content-Type': 'application/json',
            },
            oidcToken: { serviceAccountEmail: EXPECTED_SERVICE_ACCOUNT_EMAIL },
          },
        },
      },
      {},
    );
    expect(client.auth.getCredentials).toHaveBeenCalledExactlyOnceWith();
  });
});
