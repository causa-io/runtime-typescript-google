import type { EventPublisher } from '@causa/runtime';
import { InjectEventPublisher, LoggerModule } from '@causa/runtime/nestjs';
import { AppFixture } from '@causa/runtime/nestjs/testing';
import { Injectable, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { Firestore } from 'firebase-admin/firestore';
import { FirebaseModule } from '../../firebase/index.js';
import { FirestoreFixture } from '../../firestore/testing.js';
import { PubSubPublisherModule } from '../../pubsub/index.js';
import { FirebaseFixture } from '../../testing.js';
import { FirestorePubSubTransactionModule } from './module.js';
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
  });
});
