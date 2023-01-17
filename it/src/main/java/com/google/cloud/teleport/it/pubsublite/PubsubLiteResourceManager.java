package com.google.cloud.teleport.it.pubsublite;

import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.TopicName;
import com.google.protobuf.ByteString;

import java.util.Map;

/** Interface for managing Pub/Sub resources in integration tests. */
public abstract class PubsubLiteResourceManager {

        /**
         * Creates a topic with the given name on Pub/Sub.
         *
         * @param topicName Topic name to create. The underlying implementation may not use the topic name
         *     directly, and can add a prefix or a suffix to identify specific executions.
         * @return The instance of the TopicName that was just created.
         */
        abstract TopicName createTopic(String topicName);

        /**
         * Creates a subscription at the specific topic, with a given name.
         *
         * @param topicName Topic Name reference to add the subscription.
         * @param subscriptionName Name of the subscription to use. Note that the underlying
         *     implementation may not use the subscription name literally, and can use a prefix or a
         *     suffix to identify specific executions.
         * @return The instance of the SubscriptionName that was just created.
         */
        abstract SubscriptionName createSubscription(TopicName topicName, String subscriptionName);

        /**
         * Publishes a message with the given data to the publisher context's topic.
         *
         * @param topic Reference to the topic to send the message.
         * @param attributes Attributes to send with the message.
         * @param data Byte data to send.
         * @return The message id that was generated.
         */
        abstract String publish(TopicName topic, Map<String, String> attributes, ByteString data)
                throws PubsubliteResourceManagerException;

        /** Delete any topics or subscriptions created by this manager. */
        abstract void cleanupAll();

        class PubsubliteResourceManagerException extends Exception {

        }

        public class DefaultPubsubliteResourceManager extends PubsubLiteResourceManager {

                @Override
                TopicName createTopic(String topicName) {
                        return null;
                }

                @Override
                SubscriptionName createSubscription(TopicName topicName, String subscriptionName) {
                        return null;
                }

                @Override
                String publish(TopicName topic, Map<String, String> attributes, ByteString data) throws PubsubliteResourceManagerException {
                        return null;
                }

                @Override
                void cleanupAll() {

                }
        }
}
