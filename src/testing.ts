import { type Fixture } from '@causa/runtime/nestjs/testing';
import { VersionedEntityFixture } from '@causa/runtime/testing';
import type { Type } from '@nestjs/common';
import { FirestoreFixture } from './firestore/testing.js';
import { AuthUsersFixture } from './identity-platform/testing.js';
import { PubSubFixture } from './pubsub/testing.js';
import { CloudSchedulerFixture } from './scheduler/testing.js';
import { SpannerFixture } from './spanner/testing.js';
import { CloudTasksFixture } from './tasks/testing.js';
import { AppCheckFixture, FirebaseFixture } from './testing.js';
import {
  FirestorePubSubTransactionRunner,
  SpannerOutboxTransactionRunner,
} from './transaction/index.js';

export * from './app-check/testing.js';
export * from './firebase/testing.js';
export * from './firestore/testing.js';
export * from './identity-platform/testing.js';
export * from './pubsub/testing.js';
export * from './spanner/testing.js';

/**
 * Creates a NestJS application using the specified module, and sets up the fixture.
 *
 * @param appModule The NestJS module to create the application from.
 * @param options Options when creating the GCP resources for the fixture.
 * @returns The {@link GoogleAppFixture}.
 */
export function createGoogleFixtures(
  options: {
    /**
     * Temporary Pub/Sub topics to create using the {@link PubSubFixture}.
     */
    pubSubTopics?: Record<string, Type>;

    /**
     * Temporary Firestore collections to create and to clear during teardown.
     */
    firestoreTypes?: Type[];

    /**
     * Spanner entities to clear during teardown.
     */
    spannerTypes?: Type[];

    /**
     * Whether the `AppCheckGuard` should be disabled.
     * Defaults to `true`.
     */
    disableAppCheck?: boolean;

    /**
     * The transaction runner to use with the created {@link VersionedEntityFixture}.
     * Defaults to {@link SpannerOutboxTransactionRunner}.
     * If `null`, no versioned entity fixture is created.
     */
    versionedEntityRunner?:
      | Type<SpannerOutboxTransactionRunner>
      | Type<FirestorePubSubTransactionRunner>
      | null;
  } = {},
): Fixture[] {
  const disableAppCheck = options.disableAppCheck ?? true;

  const versionedEntityFixture =
    options.versionedEntityRunner !== null
      ? [
          new VersionedEntityFixture(
            options.versionedEntityRunner ?? SpannerOutboxTransactionRunner,
            PubSubFixture,
          ),
        ]
      : [];

  return [
    new FirebaseFixture(),
    new AuthUsersFixture(),
    new FirestoreFixture(options.firestoreTypes ?? []),
    new SpannerFixture({ types: options.spannerTypes }),
    new PubSubFixture(options.pubSubTopics ?? {}),
    ...(disableAppCheck ? [new AppCheckFixture()] : []),
    ...versionedEntityFixture,
    new CloudTasksFixture(),
    new CloudSchedulerFixture(),
  ];
}
