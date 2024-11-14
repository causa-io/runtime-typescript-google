import firebaseAdmin from 'firebase-admin';
import { type App, initializeApp } from 'firebase-admin/app';

/**
 * Returns the default Firebase {@link App} that should be used when initializing Firebase clients.
 * In a NestJS application, prefer using the `FirebaseModule` instead.
 */
export function getDefaultFirebaseApp(): App {
  return firebaseAdmin.apps[0] ?? initializeApp();
}
