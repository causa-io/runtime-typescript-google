# 🔖 Changelog

## Unreleased

## v0.36.0 (2025-02-14)

Features:

- Accept `nestApplicationOptions` when creating a `GoogleAppFixture`.

## v0.35.4 (2025-01-06)

Chores:

- Upgrade dependencies.

## v0.35.3 (2024-12-04)

Fixes:

- Adapt to the Causa runtime breaking changes and avoid `OutboxEventTransaction` reuse.

## v0.35.2 (2024-12-03)

Chores:

- Use the Causa `Logger` and set the context in the `PubSubEventHandlerInterceptor`.
- Change `PubSubPublisher` logs to avoid conflicting with the `PubSubEventHandlerInterceptor`.
- Catch and log errors when closing the Spanner client during shutdown.

## v0.35.1 (2024-12-02)

Chores:

- Overwrite the default outbox lease duration when the publisher is Pub/Sub.

## v0.35.0 (2024-11-29)

Breaking changes:

- Configure the `PubSubPublisher` using a Causa `(Pino)Logger`.

Chores:

- Set the context for all loggers.

Fixes:

- Fix suggested DDL for `SpannerOutboxEvent`.

## v0.34.0 (2024-11-27)

Breaking changes:

- Move and rename the `SpannerTransaction` (from `SpannerPubSubTransaction`) for reuse.

Features:

- Implement new `PreparedEvent` feature for the `PubSubPublisher`.
- Define the `SpannerReadWriteTransaction` type to avoid relying directly on the Spanner client.
- Implement the `SpannerOutboxTransactionRunner`.

## v0.33.1 (2024-11-18)

Fixes:

- Export `HttpMethod` as value and not just type.

## v0.33.0 (2024-11-14)

Features:

- Officially support Node.js 22.

Chores:

- Upgrade dependencies.

## v0.32.1 (2024-09-19)

Fix:

- Revert the `FirestoreHealthIndicator` getting the database, as this is counted as a database operation with an extremely low quota.

## v0.32.0 (2024-09-19)

Features:

- Make the `FirebaseModule` provide the `FirestoreAdminClient`.
- Make the `FirestoreHealthIndicator` get the database instead of listing collections.

## v0.31.0 (2024-09-13)

Features:

- Implement `CloudTasksScheduler.getQueuePath`, to get the full resource name for a queue from the configuration.

## v0.30.0 (2024-09-04)

Features:

- Export `getFirestoreCollectionInjectionName` to get the injection token for a Firestore collection.

## v0.29.0 (2024-07-19)

Features:

- Support testing Pub/Sub attributes in higher-level test functions (`PubSubFixture.expectEventInTopic` and `GoogleAppFixture.expectMutatedVersionedEntity`).

## v0.28.0 (2024-07-04)

Features:

- Redact the `X-Firebase-AppCheck` header by default in the `googlePinoConfiguration`.

## v0.27.1 (2024-05-30)

Fixes:

- Add `RESOURCE_EXHAUSTED` to the list of retryable statuses for Spanner errors.
- Close the Pub/Sub client after having deleted all topic fixtures.

## v0.27.0 (2024-05-24)

Features:

- Provide the `overrideAppCheck` testing utility.
- Disable the `AppCheckGuard` by default in the `GoogleAppFixture`.

## v0.26.0 (2024-05-06)

Features:

- Provide Firebase `Messaging` in the `FirebaseModule`.

## v0.25.0 (2024-05-03)

Breaking changes:

- Remove the `PubSubEventHandlerModule`, as "interceptors are not providers". Prefer using `APP_INTERCEPTOR` or `@UseInterceptors` instead.

Features:

- Add the `withSerializer` static method to the `PubSubEventHandlerInterceptor`.

## v0.24.1 (2024-04-02)

Fixes:

- Loosen type constraint on `GoogleAppFixture.expectNonMutatedVersionedEntity`.

## v0.24.0 (2024-04-02)

Breaking changes:

- Change types for versioned entity tests in the `GoogleAppFixture`.

Fixes:

- Do not remove interleaved rows when updating an entity using `SpannerEntityManager.update`.

## v0.23.0 (2024-04-01)

Features:

- Support Firestore documents/collections which are not decorated with `@SoftDeletedFirestoreCollection` in the `FirestoreStateTransaction`.

## v0.22.0 (2024-03-19)

Breaking changes:

- `PubSubHealthIndicator` and `SpannerHealthIndicator` now extend the `BaseHealthIndicatorService`.
- Remove the `GoogleHealthcheckModule`.

Features:

- Implement the `FirestoreHealthIndicator`.

## v0.21.0 (2024-03-15)

Features:

- Support an optional `code` parameter for `TemporarySpannerError` and `TemporaryFirestoreError`, and provide a `retryableInTransaction` utility for both of them.

Fixes:

- Ensure all retryable errors cases are handled for Firestore and Spanner.

## v0.20.0 (2024-03-06)

Chores:

- Upgrade dependencies to keep in sync with `@causa/runtime`.

## v0.19.0 (2024-02-08)

Features:

- Implement the `PubSubEventPublishTime` decorator, to retrieve the `publishTime` for the message.

## v0.18.0 (2024-01-22)

Breaking changes:

- Use new `supertest` types, which makes the main object a `TestAgent`.

Chores:

- Upgrade dependencies to keep in sync with `@causa/runtime`.

## v0.17.0 (2023-12-11)

Breaking changes:

- Adapt the `PubSubEventHandlerInterceptor` and the `PubSubPublisher` to breaking changes in the Causa runtime.

Features:

- Add option for the `PubSubEventHandlerModule` not to set the interceptor globally.

## v0.16.0 (2023-11-28)

Features:

- Loosen `makePubSubRequester`'s `EventRequester` to allow testing arbitrary events, which don't conform to the `Event` interface.

## v0.15.0 (2023-11-24)

Features:

- Rethrow transient errors as `RetryableError`s in the `CloudTasksScheduler`.
- Implement the `convertFirestoreError` and `wrapFirestoreOperation` utilities.
- Use `wrapFirestoreOperation` in the `FirestorePubSubTransactionRunner` to catch and rethrow Firestore errors as entity errors or `RetryableError`s.

## v0.14.0 (2023-11-08)

Breaking changes:

- Make the Pub/Sub `EventRequester` accept an options object. In addition to the previous `expectedStatus` argument, it also supports passing message attributes in the request.

## v0.13.0 (2023-10-02)

Breaking changes:

- Use NestJS `Type` for all references to class types.

Features:

- Expose the `Spanner` client from the `SpannerModule`.
- Manage the `Spanner` client in the `GoogleAppFixture`.
- Make the failed `PubSubFixture` event expectation clearer.
- Add a default delay before checking that no message has been published to a topic in `PubSubFixture.expectNoMessageInTopic`.

Fix:

- Close the `Spanner` client and `Database` when the application terminates.

## v0.12.0 (2023-09-19)

Breaking changes:

- Entities returned by the `SpannerEntityManager` are now created by `class-transformer`. This means `@Transform` and `@Type` decorators can be used (e.g. to process the objects JSON columns, like dates).

## v0.11.0 (2023-09-13)

Features:

- Disable batching in the `PubSubPublisher` by default, and allow per topic configuration.
- Support `MakeTestAppFactoryOptions` when creating a `GoogleAppFixture`.

Fixes:

- Exclude the Google healthcheck endpoint from the OpenAPI documentation.

## v0.10.0 (2023-09-12)

Features:

- Log main steps of Spanner Pub/Sub and Firestore Pub/Sub transactions.

## v0.9.0 (2023-09-11)

Breaking changes:

- Remove unnecessary generic for the `EventRequester`.

Features:

- Implement the `GoogleAppFixture`.
- Export the `SpannerKey` type.

Fixes:

- Remove users from the `AuthUsersFixture` when calling `deleteAll`.

## v0.8.1 (2023-09-08)

Fixes:

- Correctly format Cloud Tasks tasks names.

## v0.8.0 (2023-09-08)

Features:

- Implement the `CloudTasksScheduler` and `CloudTasksModule`.

## v0.7.0 (2023-08-31)

Breaking changes:

- Upgrade `@google-cloud` dependencies (e.g. Pub/Sub, Spanner) to latest major versions.

## v0.6.0 (2023-07-26)

Breaking changes:

- Accept a permission denied error as a healthy check for Pub/Sub.

## v0.5.0 (2023-07-21)

Features:

- Handle multiple inserts and replacements at once in `SpannerEntityManager.insert` and `SpannerEntityManager.replace`.
- Define query parameters type hints for the Spanner client `SpannerEntityManager.ParamType*Array`.

## v0.4.0 (2023-07-17)

Features:

- Support `index` and `disableQueryNullFilteredIndexEmulatorCheck` options in `SpannerEntityManager.sqlTableName`.

## v0.3.0 (2023-07-12)

Breaking changes:

- Make the `overrideFirestoreCollections` testing utility return a `NestJsModuleOverrider`, for consistency with other overrides.

## v0.2.0 (2023-07-12)

Features:

- Implement the `overrideFirebaseApp`, `PubSubFixture`, `overrideDatabase` overrider utilities for testing.
- Implement the `PubSubFixture.expectEventInTopic` testing utility.
- Support a soft delete column in the `SpannerColumn` decorator and `SpannerEntityManager`.
- Implement `sqlTableName` and `sqlColumns` utilities in the `SpannerEntityManager`.
- Implement the `GoogleHealthcheckModule`.

Fixes:

- Export the `FirestorePubSubTransactionModule`.
- Ensure the project ID is populated in the `PubSubFixture`.

## v0.1.0 (2023-07-10)

Features:

- Implement the `IsValidFirestoreId` decorator.
- Implement the NestJS `FirebaseModule`.
- Implement the `getDefaultFirebaseApp()` function.
- Implement the `IdentityPlatformStrategy`.
- Implement Identity Platform-related testing utilities.
- Define the logger configuration for GCP.
- Implement the `PubSubPublisher`, the `PubSubEventHandlerInterceptor`, and the corresponding modules.
- Implement the `PubSubHealthIndicator`.
- Implement the `AppCheckGuard`.
- Implement the Firestore utilities, including testing and dependency injection for NestJS.
- Implement the `SpannerEntityManager` and related utilities.
- Implement the `SpannerModule` and `SpannerHealthIndicator`.
- Implement the `SpannerPubSubTransactionRunner` and the `SpannerPubSubTransactionModule`.
- Implement the `FirestorePubSubTransactionRunner` and the `FirestorePubSubTransactionModule`.
