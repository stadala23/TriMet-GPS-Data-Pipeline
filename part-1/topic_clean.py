from google.cloud import pubsub_v1

# TODO(developer)
project_id = "dataeng-saheli-bavirisetty"
subscription_id = "my-sub"

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    # Discard the message
    message.ack()
    print(f"Discarded message {message.message_id}")

# Subscribe to the given topic and receive messages
subscriber.subscribe(subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}...")

# Keep the program running to continue listening for messages
while True:
    pass


