/**
 * The prefix of environment variables containing Pub/Sub topic IDs.
 */
export const PUBSUB_TOPIC_CONFIG_VAR_PREFIX = 'PUBSUB_TOPIC_';

/**
 * Gets the name of the configuration key containing the Pub/Sub topic ID for a given topic.
 *
 * @param topicName The name of the event topic.
 * @returns The name of the configuration key containing the Pub/Sub topic ID.
 */
export function getConfigurationKeyForTopic(topicName: string): string {
  return `${PUBSUB_TOPIC_CONFIG_VAR_PREFIX}${topicName
    .toUpperCase()
    .replace(/[-\.]/g, '_')}`;
}
