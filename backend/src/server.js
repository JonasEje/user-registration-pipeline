import path from "node:path";
import { fileURLToPath } from "node:url";

import { createApp } from "./app.js";
import { loadConfig } from "./config.js";
import { createRegistrationPublisher } from "./kafka.js";
import { connectMongo } from "./mongo.js";

const MODULE_DIRECTORY = path.dirname(fileURLToPath(import.meta.url));
const ENV_FILE = path.resolve(MODULE_DIRECTORY, "..", "..", ".env");

function loadEnvFile() {
  try {
    process.loadEnvFile(ENV_FILE);
  } catch (error) {
    if (error.code !== "ENOENT") {
      throw error;
    }
  }
}

async function main() {
  loadEnvFile();
  const config = loadConfig();

  const mongoClient = await connectMongo({ uri: config.mongoUri });
  const collection = mongoClient
    .db(config.mongoDatabase)
    .collection(config.mongoCollection);

  const publisher = createRegistrationPublisher({
    brokers: config.kafkaBrokers,
    topic: config.kafkaTopic,
  });
  await publisher.connect();

  const app = createApp({
    publisher,
    collection,
    frontendDir: config.frontendDir,
  });
  const server = app.listen(config.port, () => {
    console.log(`Backend listening on http://localhost:${config.port}`);
  });

  let shuttingDown = false;
  async function shutdown() {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;

    await new Promise((resolve) => server.close(resolve));
    await publisher.close();
    await mongoClient.close();
  }

  process.on("SIGINT", async () => {
    await shutdown();
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    await shutdown();
    process.exit(0);
  });
}

main().catch((error) => {
  console.error("Backend failed to start:", error.message);
  process.exit(1);
});
