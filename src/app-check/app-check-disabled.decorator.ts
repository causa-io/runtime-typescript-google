import { SetMetadata } from '@nestjs/common';

/**
 * The metadata key that is added for routes with AppCheck disabled.
 */
export const APP_CHECK_DISABLED_METADATA_KEY = 'appCheckDisabled';

/**
 * Disables App Check for the decorated route(s).
 * Can decorate a controller or one of its methods.
 */
export const AppCheckDisabled = () =>
  SetMetadata(APP_CHECK_DISABLED_METADATA_KEY, true);
