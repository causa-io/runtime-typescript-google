import { v1 } from 'firebase-admin/firestore';

/**
 * The low-level client to access administrative Firestore operations.
 */
export const FirestoreAdminClient = v1.FirestoreAdminClient;
export type FirestoreAdminClient = InstanceType<typeof v1.FirestoreAdminClient>;
