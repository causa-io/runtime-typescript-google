import { Inject } from '@nestjs/common';

/**
 * The NestJS token used to inject the Firebase `App`.
 */
export const FIREBASE_APP_TOKEN = 'CAUSA_FIREBASE';

/**
 * Decorates a parameter or property to inject the Firebase `App`.
 */
export const InjectFirebaseApp = () => Inject(FIREBASE_APP_TOKEN);
