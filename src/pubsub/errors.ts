/**
 * An error thrown when publishing to a Pub/Sub topic that is not configured.
 */
export class PubSubTopicNotConfiguredError extends Error {
  constructor(readonly topicName: string) {
    super(`Pub/Sub topic ID for topic '${topicName}' is not configured.`);
  }
}
