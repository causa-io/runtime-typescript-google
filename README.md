# Causa TypeScript runtime SDK for Google services

## ➕ Requirements

This package is intended to be run in an environment configured with Google services. This could be a Cloud Function, a Cloud Run container, etc. This can also be a local environment set up with the relevant emulators.

In Google-provided environments, authentication to Google services should be automatic. Locally, you can use the Causa CLI with the [Google module](https://github.com/causa-io/workspace-module-google) to easily run emulators. See the configuration section for more details.

## 🎉 Installation

The package can be added like any other to a Node.js project:

```
npm install @causa/runtime-google
```

## 🔧 Configuration

Here is an example of the environment variables that are used by this package:

```
# The name of the Spanner instance configured by the `SpannerModule`.
SPANNER_INSTANCE=instance
# The name of the Spanner database configured by the `SpannerModule`.
SPANNER_DATABASE=database
# An example of the expected format for environment variables containing Pub/Sub topic IDs (for publishing).
PUBSUB_TOPIC_SOME_TOPIC_NAME=projects/demo-causa/topics/some-topic.name

# Google client variables.
# Various ways of setting the GCP project ID. `GOOGLE_CLOUD_PROJECT` is used by the `AuthUsersFixture` (only relevant for testing).
GOOGLE_CLOUD_PROJECT=demo-causa
GCP_PROJECT=demo-causa
GCLOUD_PROJECT=demo-causa

# Emulator-related variables, used for local development.
FIREBASE_AUTH_EMULATOR_HOST=127.0.0.1:9099
FIREBASE_STORAGE_EMULATOR_HOST=127.0.0.1:9199
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080
SPANNER_EMULATOR_HOST=127.0.0.1:9010
PUBSUB_EMULATOR_HOST=127.0.0.1:8085
```

## ✨ Features

### Firebase AppCheck NestJS guard

The `AppCheckGuard` provides a NestJS app guard which verifies [Firebase AppCheck](https://firebase.google.com/docs/app-check) tokens on routes, unless decorated with `@AppCheckDisabled`. The token must be set in the `X-Firebase-AppCheck` header of HTTP requests.

### Firebase NestJS module

The `FirebaseModule` is a NestJS module which exports various Firebase services for injection: `Auth`, `Firestore`, and `AppCheck`. The `@InjectFirebaseApp` decorator can be used to retrieve the parent Firebase application.

Outside of a NestJS context, `getDefaultFirebaseApp()` can be used to retrieve a consistently initialized singleton app.

For testing, `overrideFirebaseApp` can be set as an `overrides` option for `makeTestAppFactory()`, which ensures the default Firebase application is used across tests and avoids lifecycle issues (repeatedly creating and tearing down new applications).

### Firestore

The `makeFirestoreDataConverter` is a utility that returns a `FirestoreDataConverter`. It converts regular TypeScript classes to Firestore documents and back. Firestore `Timestamp`s are converted to `Date`s, and `class-transformer` decorators are applied.

The `FirestoreCollectionsModule` builds upon the `FirebaseModule` and provides Firestore collections for the listed document types. In services, the `@InjectFirestoreCollection` decorator can be used to retrieve a Firestore collection, prepared with the aforementioned converter.

Testing utilities are also provided. `clearFirestoreCollection()` can be used in between tests to reinitialize the collection. `overrideFirestoreCollections` can be used as an `overrides` option for `makeTestAppFactory()` to replace collections with temporary ones. This ensures separate collections are used for each test and avoids conflicts.

### NestJS health checks

The `GoogleHealthcheckModule` is a ready-to-use module that includes the Spanner and Pub/Sub health checks. This module can be used if no other health check is needed in the application. Otherwise, you can combine the individual health checks into your own controller and module.

### Identity Platform Passport strategy

The `IdentityPlatformStrategy` is a Passport strategy for NestJS which verifies bearer tokens as Identity Platform ID tokens. It returns the decoded token as a `User` object, with an `id` and possibly claims.

For testing, the `AuthUsersFixture` provides a way to easily create and delete Identity Platform users, as well as generate tokens for them. This utility requires the `GOOGLE_CLOUD_PROJECT` environment variable to properly construct the tokens.

### Logger configuration for Cloud Logging and Error Reporting

The `googlePinoConfiguration` provides options which can be applied using `updatePinoConfiguration()` to match Cloud Logging expectations (the logging level as a `severity` field in the JSON logs). It also tags error-level logs such that they are picked up by Cloud Logging.

### Pub/Sub

The `PubSubPublisherModule` provides the `PubSub` client, as well as the `PubSubPublisher`, which implements the `EventPublisher` interface. As such, it can be injected as either `PubSubPublisher`, or using the `@InjectEventPublisher` decorator.

The `PubSubPublisher` requires the `PUBSUB_TOPIC_*` environment variables to be set for all the topics a service is expected to publish to. The name of the environment variables should be prefixed with `PUBSUB_TOPIC_`, followed by the topic full name in upper case, and using `_` as the only punctuation. For example, `my-domain.my-topic.v1` would become `PUBSUB_TOPIC_MY_DOMAIN_MY_TOPIC_V1`.

For services being triggered by Pub/Sub messages, the `PubSubEventHandlerModule` can be used to automatically parse Pub/Sub messages coming from HTTP requests made by a Pub/Sub push subscription. Any route with a parameter decorated with `@EventBody` will trigger the `PubSubEventHandlerInterceptor`, and will receive the parsed event pushed by Pub/Sub.

The `PubSubHealthIndicator` is a `HealthIndicator` which can be used in a health check controller, such as the `GoogleHealthcheckModule`. It attempts to list topics using the Pub/Sub client to check connectivity to the Pub/Sub API.

To test publishers, the `PubSubFixture` handles the creation and deletion of temporary topics. The `PubSubFixture.createWithOverrider()` method is especially useful when used in combination with the `makeTestAppFactory()` utility.

To test event handlers, the `makePubSubRequester()` utility returns a function which can be used to make HTTP requests in the same way a Pub/Sub push subscription would.

### Spanner

The `SpannerEntityManager` is an entity manager having some similarities with TypeORM, but with a much more limited feature set. It handles entity classes decorated using `@SpannerTable` and `@SpannerColumn`.

The `SpannerModule` provides a `Database` instance configured using the `SPANNER_INSTANCE` and `SPANNER_DATABASE` environment variables, and the `SpannerEntityManager`.

The `SpannerHealthIndicator` is a `HealthIndicator` which can be used in a health check controller, such as the `GoogleHealthcheckModule`. It runs a dummy `SELECT 1` query against the database to check connectivity.

For testing, the `createDatabase` utility creates a temporary database, copying the DDL from the configured database (set with the `SPANNER_DATABASE` environment variable). `overrideDatabase` can be used as an `overrides` option for `makeTestAppFactory` to substitute the database with a temporary one.

### GCP-based Causa transaction runners

This package provides two `TransactionRunner`s: the `SpannerPubSubTransactionRunner` and the `FirestorePubSubTransactionRunner`. Both use Pub/Sub and a `BufferEventTransaction` to publish events. They only differ by the service used to store the state.

The `SpannerPubSubTransactionRunner` uses a Spanner transaction as the underlying transaction for the `SpannerPubSubTransaction`, while the `FirestorePubSubTransactionRunner` uses a Firestore transaction for the `FirestorePubSubTransaction`. Both state transactions implement the `FindReplaceStateTransaction` interface, and therefore the runners can be used with the `VersionedEntityManager`.

One feature sets the `FirestorePubSubTransactionRunner` and its `FirestoreStateTransaction` apart: the handling of deleted entities using a separate, "soft-deleted document collection". Entities with a non-null `deletedAt` property are moved to a collection suffixed with `$deleted`, and an `_expirationDate` field is added to them. A TTL is expected to be set on this field. The `@SoftDeletedFirestoreCollection` decorator must be added to document classes that are meant to be handled using the `FirestorePubSubTransactionRunner`.

### Validation

The `@IsValidFirestoreId` validation decorator checks that a property is a string which is not `.` or `..`, and does not contain forward slashes. This ensures the property's value can be used as a Firestore document ID.
