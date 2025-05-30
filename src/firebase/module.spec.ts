import { jest } from '@jest/globals';
import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { type App, deleteApp } from 'firebase-admin/app';
import { AppCheck } from 'firebase-admin/app-check';
import { Auth, getAuth } from 'firebase-admin/auth';
import { Firestore, v1 } from 'firebase-admin/firestore';
import { Messaging } from 'firebase-admin/messaging';
import 'jest-extended';
import { getDefaultFirebaseApp } from './app.js';
import { FirestoreAdminClient } from './firestore-admin-client.type.js';
import { InjectFirebaseApp } from './inject-firebase-app.decorator.js';
import { FirebaseModule, type FirebaseModuleOptions } from './module.js';

describe('FirebaseModule', () => {
  @Injectable()
  class MyService {
    constructor(
      @InjectFirebaseApp()
      readonly app: App,
      readonly firestore: Firestore,
      readonly auth: Auth,
      readonly appCheck: AppCheck,
      readonly messaging: Messaging,
      readonly firestoreAdmin: FirestoreAdminClient,
    ) {}
  }

  let testModule: TestingModule;
  let service: MyService;

  async function createInjectedService(
    options?: FirebaseModuleOptions | 'testing',
  ): Promise<void> {
    testModule = await Test.createTestingModule({
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

  it('should inject the Firestore client and configure it with default options', async () => {
    await createInjectedService();

    expect(service.firestore).toBeInstanceOf(Firestore);
    expect((service.firestore as any)._settings).toMatchObject({
      ignoreUndefinedProperties: true,
    });
  });

  it('should inject the Auth client', async () => {
    await createInjectedService();

    expect(service.auth).toBeInstanceOf(Auth);
  });

  it('should inject the AppCheck client', async () => {
    await createInjectedService();

    expect(service.appCheck).toBeInstanceOf(AppCheck);
  });

  it('should inject the Messaging client', async () => {
    await createInjectedService();

    expect(service.messaging).toBeInstanceOf(Messaging);
  });

  it('should inject the FirestoreAdminClient', async () => {
    await createInjectedService();

    expect(service.firestoreAdmin).toBeInstanceOf(v1.FirestoreAdminClient);
  });

  it('should use options when initializing the app', async () => {
    await createInjectedService({
      appName: '🤖',
      projectId: 'demo-project',
      firestore: { ignoreUndefinedProperties: false },
    });

    expect(service.app).toBeDefined();
    expect(service.app.name).toEqual('🤖');
    expect(service.app.options.projectId).toEqual('demo-project');
    expect((service.firestore as any)._settings).toMatchObject({
      ignoreUndefinedProperties: false,
    });
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

  it('should terminate Firebase clients and the app when the application shuts down', async () => {
    await createInjectedService();
    jest.spyOn(service.firestore, 'terminate');

    await testModule.close();

    expect(service.firestore.terminate).toHaveBeenCalledOnce();
    expect(() => getAuth(service.app)).toThrow('has already been deleted.');
  });
});
