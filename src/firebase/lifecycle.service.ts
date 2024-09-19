import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { App, deleteApp } from 'firebase-admin/app';
import { Firestore } from 'firebase-admin/firestore';
import { FirestoreAdminClient } from './firestore-admin-client.type.js';
import { InjectFirebaseApp } from './inject-firebase-app.decorator.js';

/**
 * A private service that handles the graceful shutdown of the Firebase App.
 * Should be imported in the `FirebaseModule`.
 */
@Injectable()
export class FirebaseLifecycleService implements OnApplicationShutdown {
  constructor(
    @InjectFirebaseApp()
    private readonly app: App,
    private readonly firestore: Firestore,
    private readonly firestoreAdmin: FirestoreAdminClient,
  ) {}

  async onApplicationShutdown(): Promise<void> {
    await this.firestoreAdmin.close();
    await this.firestore.terminate();
    await deleteApp(this.app);
  }
}
