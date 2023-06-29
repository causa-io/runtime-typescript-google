import { Test } from '@nestjs/testing';
import { Injectable } from '@nestjs/common';
import { App, deleteApp } from 'firebase-admin/app';
import { AppCheck } from 'firebase-admin/app-check';
import { Auth } from 'firebase-admin/auth';
import { Firestore } from 'firebase-admin/firestore';
import 'jest-extended';
import { getDefaultFirebaseApp } from './app.js';
import { InjectFirebaseApp } from './inject-firebase-app.decorator.js';
import { FirebaseModule, FirebaseModuleOptions } from './module.js';

describe('FirebaseModule', () => {
  @Injectable()
  class MyService {
    constructor(
      @InjectFirebaseApp()
      readonly app: App,
      readonly firestore: Firestore,
      readonly auth: Auth,
      readonly appCheck: AppCheck,
    ) {}
  }

  let service: MyService;

  async function createInjectedService(
    options?: FirebaseModuleOptions | 'testing',
  ): Promise<void> {
    const testModule = await Test.createTestingModule({
      imports: [
        options === 'testing'
          ? FirebaseModule.forTesting()
          : options === undefined
          ? FirebaseModule
          : FirebaseModule.forRoot(options),
      ],
      providers: [MyService],
    }).compile();

    service = testModule.get(MyService);
  }

  afterEach(async () => {
    try {
      await deleteApp(service.app);
    } catch (error: any) {
      if (error.code !== 'app/app-deleted') {
        throw error;
      }
    }
  });

  it('should inject the Firebase App', async () => {
    await createInjectedService();

    expect(service.app).toBeDefined();
    expect(service.app.name).toBeString();
  });

  it('should inject the Firestore client', async () => {
    await createInjectedService();

    expect(service.firestore).toBeInstanceOf(Firestore);
  });

  it('should inject the Auth client', async () => {
    await createInjectedService();

    expect(service.auth).toBeInstanceOf(Auth);
  });

  it('should inject the AppCheck client', async () => {
    await createInjectedService();

    expect(service.appCheck).toBeInstanceOf(AppCheck);
  });

  it('should use options when initializing the app', async () => {
    await createInjectedService({ appName: '🤖', projectId: 'demo-project' });

    expect(service.app).toBeDefined();
    expect(service.app.name).toEqual('🤖');
    expect(service.app.options.projectId).toEqual('demo-project');
  });

  it('should create a global module', async () => {
    const actualModule = FirebaseModule.forRoot();

    expect(actualModule).toMatchObject({ global: true });
  });

  it('should create a non-global module', async () => {
    const actualModule = FirebaseModule.register();

    expect(actualModule.global).toBeFalsy();
  });

  it('should create a module using the default Firebase app', async () => {
    const expectedApp = getDefaultFirebaseApp();

    await createInjectedService('testing');

    expect(service.app).toBe(expectedApp);
  });
});
