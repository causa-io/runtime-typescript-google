/**
 * Additional information expected to be present on an express request that was parsed as a Pub/Sub message.
 */
export type RequestWithPubSubInfo = {
  /**
   * The `publishTime` of the Pub/Sub message.
   */
  pubSubPublishTime: Date;
};
