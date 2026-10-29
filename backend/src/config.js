import path from "node:path";
import { fileURLToPath } from "node:url";

const MODULE_DIRECTORY = path.dirname(fileURLToPath(import.meta.url));

export const PROJECT_ROOT = path.resolve(MODULE_DIRECTORY, "..", "..");

export function readEnv(env, name, fallback) {
  const rawValue = env[name];
  const value = rawValue === undefined ? "" : rawValue.trim();
  if (value === "") {
    if (fallback === undefined) {
      throw new Error(`Missing required environment variable: ${name}`);
    }
    return fallback;
  }
  return value;
}

export function buildMongoUri(env = process.env) {
  const explicitUri = readEnv(env, "MONGO_URI", "");
  if (explicitUri !== "") {
    return explicitUri;
  }

  const username = encodeURIComponent(readEnv(env, "MONGO_ROOT_USERNAME"));
  const password = encodeURIComponent(readEnv(env, "MONGO_ROOT_PASSWORD"));
  const host = readEnv(env, "MONGO_HOST", "localhost");
  const port = readEnv(env, "MONGO_PORT", "27017");
  return `mongodb://${username}:${password}@${host}:${port}/?authSource=admin`;
}

export function loadConfig(env = process.env) {
  const port = Number.parseInt(readEnv(env, "PORT", "3000"), 10);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("PORT must be an integer between 1 and 65535");
  }

  const kafkaBrokers = readEnv(
    env,
    "KAFKA_BOOTSTRAP_SERVERS",
    "127.0.0.1:9094",
  )
    .split(",")
    .map((broker) => broker.trim())
    .filter((broker) => broker !== "");

  if (kafkaBrokers.length === 0) {
    throw new Error("KAFKA_BOOTSTRAP_SERVERS must list at least one broker");
  }

  return {
    port,
    kafkaBrokers,
    kafkaTopic: readEnv(env, "KAFKA_TOPIC", "user-registrations"),
    mongoUri: buildMongoUri(env),
    mongoDatabase: readEnv(env, "MONGO_DATABASE", "UserRegistrationDB"),
    mongoCollection: readEnv(env, "MONGO_COLLECTION", "User"),
    frontendDir: path.join(PROJECT_ROOT, "frontend"),
  };
}
