# 🔖 Changelog

## Unreleased

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
