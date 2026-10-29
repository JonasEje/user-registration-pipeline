import { Kafka } from "kafkajs";

export function createRegistrationPublisher({ brokers, topic }) {
  const kafka = new Kafka({
    clientId: "user-registration-backend",
    brokers,
  });
  const producer = kafka.producer();
  let connected = false;

  return {
    async connect() {
      if (!connected) {
        await producer.connect();
        connected = true;
      }
    },
    async publish(registration) {
      await producer.send({
        topic,
        messages: [
          {
            key: registration._id,
            value: JSON.stringify(registration),
          },
        ],
      });
    },
    async close() {
      if (connected) {
        await producer.disconnect();
        connected = false;
      }
    },
  };
}
