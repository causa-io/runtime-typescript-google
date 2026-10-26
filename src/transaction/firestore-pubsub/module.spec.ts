import type { EventPublisher } from '@causa/runtime';
import { InjectEventPublisher, LoggerModule } from '@causa/runtime/nestjs';
import { AppFixture, ConfigFixture } from '@causa/runtime/nestjs/testing';
import { Injectable, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { Firestore } from 'firebase-admin/firestore';
import { FirebaseModule } from '../../firebase/index.js';
import { FirestoreFixture } from '../../firestore/testing.js';
import { PubSubPublisherModule } from '../../pubsub/index.js';
import { FirebaseFixture } from '../../testing.js';
import {
  FirestorePubSubTransactionModule,
  type FirestorePubSubTransactionModuleOptions,
} from './module.js';
import { FirestorePubSubTransactionRunner } from './runner.js';

@Injectable()
class MyService {
  constructor(
    @InjectEventPublisher()
    readonly publisher: EventPublisher,
    readonly runner: FirestorePubSubTransactionRunner,
  ) {}
}

@Module({
  providers: [MyService],
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    LoggerModule.forRoot(),
    PubSubPublisherModule.forRoot(),
    FirebaseModule.forTesting(),
    FirestorePubSubTransactionModule.forRoot(),
  ],
})
export class MyModule {}

describe('FirestorePubSubTransactionModule', () => {
  let appFixture: AppFixture;

  beforeEach(async () => {
    appFixture = new AppFixture(MyModule, {
      fixtures: [new FirebaseFixture(), new FirestoreFixture()],
    });
    await appFixture.init();
  });

  afterEach(() => appFixture.delete());

  it('should expose the runner', async () => {
    const { runner: actualRunner } = appFixture.get(MyService);

    expect(actualRunner).toBeInstanceOf(FirestorePubSubTransactionRunner);
    expect(actualRunner.firestore).toBe(appFixture.get(Firestore));
    expect(actualRunner.stateTransactionOptions).toEqual({
      sequentialSoftDeleteLookups: false,
    });
  });

  describe('options', () => {
    let optionsAppFixture: AppFixture | undefined;

    afterEach(async () => {
      await optionsAppFixture?.delete();
      optionsAppFixture = undefined;
    });

    async function createRunner(
      options: FirestorePubSubTransactionModuleOptions | undefined,
      config: Record<string, string>,
    ): Promise<FirestorePubSubTransactionRunner> {
      @Module({
        imports: [
          ConfigModule.forRoot({ isGlobal: true }),
          LoggerModule.forRoot(),
          PubSubPublisherModule.forRoot(),
          FirebaseModule.forTesting(),
          FirestorePubSubTransactionModule.forRoot(options),
        ],
      })
      class OptionsModule {}

      optionsAppFixture = new AppFixture(OptionsModule, {
        fixtures: [new ConfigFixture(config), new FirebaseFixture()],
      });
      await optionsAppFixture.init();
      return optionsAppFixture.get(FirestorePubSubTransactionRunner);
    }

    it('should read the options from the environment', async () => {
      const actualRunner = await createRunner(undefined, {
        FIRESTORE_SEQUENTIAL_SOFT_DELETE_LOOKUPS: 'true',
      });

      expect(actualRunner.stateTransactionOptions).toEqual({
        sequentialSoftDeleteLookups: true,
      });
    });

    it('should prefer the options passed to the module', async () => {
      const actualRunner = await createRunner(
        { sequentialSoftDeleteLookups: false },
        { FIRESTORE_SEQUENTIAL_SOFT_DELETE_LOOKUPS: 'true' },
      );

      expect(actualRunner.stateTransactionOptions).toEqual({
        sequentialSoftDeleteLookups: false,
      });
    });
  });
});
