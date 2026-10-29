import assert from "node:assert/strict";
import path from "node:path";
import { after, before, describe, it } from "node:test";

import { createApp } from "../src/app.js";
import {
  PROJECT_ROOT,
  buildMongoUri,
  loadConfig,
} from "../src/config.js";
import {
  buildDailyCountsPipeline,
  buildRegistration,
  loadDailyCounts,
} from "../src/registrations.js";

const FRONTEND_DIR = path.join(PROJECT_ROOT, "frontend");
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

function startServer(app) {
  return new Promise((resolve) => {
    const server = app.listen(0, "127.0.0.1", () => {
      const address = server.address();
      resolve({
        server,
        baseUrl: `http://127.0.0.1:${address.port}`,
      });
    });
  });
}

function stopServer(server) {
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

function createCollection(rows) {
  return {
    aggregate(pipeline) {
      return {
        toArray: async () => {
          assert.deepEqual(pipeline, buildDailyCountsPipeline());
          return rows;
        },
      };
    },
  };
}

describe("buildRegistration", () => {
  it("creates a UUID and a UTC timestamp", () => {
    const now = new Date("2026-10-29T12:34:56.789Z");

    const registration = buildRegistration(now);

    assert.match(registration._id, UUID_PATTERN);
    assert.equal(registration.registered_at, "2026-10-29T12:34:56.789Z");
    assert.deepEqual(Object.keys(registration).sort(), [
      "_id",
      "registered_at",
    ]);
  });
});

describe("loadDailyCounts", () => {
  it("maps aggregation rows to API fields", async () => {
    const collection = createCollection([
      { _id: "2026-10-28", new_users: 1 },
      { _id: "2026-10-29", new_users: 4 },
    ]);

    const days = await loadDailyCounts(collection);

    assert.deepEqual(days, [
      { registration_date: "2026-10-28", new_users: 1 },
      { registration_date: "2026-10-29", new_users: 4 },
    ]);
  });
});

describe("buildMongoUri", () => {
  it("builds an authenticated URI from parts", () => {
    const uri = buildMongoUri({
      MONGO_ROOT_USERNAME: "projectadmin",
      MONGO_ROOT_PASSWORD: "p@ss word",
      MONGO_HOST: "mongodb",
      MONGO_PORT: "27018",
    });

    assert.equal(
      uri,
      "mongodb://projectadmin:p%40ss%20word@mongodb:27018/?authSource=admin",
    );
  });

  it("prefers an explicit MONGO_URI", () => {
    const uri = buildMongoUri({
      MONGO_URI: "mongodb://example.test:27017",
    });

    assert.equal(uri, "mongodb://example.test:27017");
  });

  it("fails fast when credentials are missing", () => {
    assert.throws(
      () => buildMongoUri({}),
      /MONGO_ROOT_USERNAME/,
    );
  });
});

describe("loadConfig", () => {
  it("applies defaults and splits broker lists", () => {
    const config = loadConfig({
      MONGO_ROOT_USERNAME: "projectadmin",
      MONGO_ROOT_PASSWORD: "secret",
      KAFKA_BOOTSTRAP_SERVERS: "127.0.0.1:9094, localhost:9095",
    });

    assert.equal(config.port, 3000);
    assert.deepEqual(config.kafkaBrokers, [
      "127.0.0.1:9094",
      "localhost:9095",
    ]);
    assert.equal(config.kafkaTopic, "user-registrations");
    assert.equal(config.mongoDatabase, "UserRegistrationDB");
    assert.equal(config.mongoCollection, "User");
    assert.equal(config.frontendDir, FRONTEND_DIR);
  });

  it("rejects an invalid port", () => {
    assert.throws(
      () =>
        loadConfig({
          MONGO_ROOT_USERNAME: "projectadmin",
          MONGO_ROOT_PASSWORD: "secret",
          PORT: "not-a-port",
        }),
      /PORT/,
    );
  });
});

describe("registration API", () => {
  const published = [];
  let server;
  let baseUrl;

  before(async () => {
    const app = createApp({
      publisher: {
        publish: async (registration) => {
          published.push(registration);
        },
      },
      collection: createCollection([
        { _id: "2026-10-28", new_users: 2 },
      ]),
      frontendDir: FRONTEND_DIR,
      logger: { error() {} },
    });
    ({ server, baseUrl } = await startServer(app));
  });

  after(async () => {
    await stopServer(server);
  });

  it("publishes a registration and returns it", async () => {
    const response = await fetch(`${baseUrl}/api/registrations`, {
      method: "POST",
    });

    assert.equal(response.status, 201);
    const body = await response.json();
    assert.equal(published.length, 1);
    assert.equal(published[0]._id, body._id);
    assert.equal(published[0].registered_at, body.registered_at);
  });

  it("returns daily counts", async () => {
    const response = await fetch(`${baseUrl}/api/registrations/stats`);

    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), {
      days: [{ registration_date: "2026-10-28", new_users: 2 }],
    });
  });

  it("serves the dashboard page", async () => {
    const response = await fetch(`${baseUrl}/`);

    assert.equal(response.status, 200);
    const html = await response.text();
    assert.match(html, /User Registrations/);
  });

  it("returns 404 for unknown API routes", async () => {
    const response = await fetch(`${baseUrl}/api/unknown`);

    assert.equal(response.status, 404);
  });
});

describe("registration API failures", () => {
  let server;
  let baseUrl;

  before(async () => {
    const app = createApp({
      publisher: {
        publish: async () => {
          throw new Error("kafka unavailable");
        },
      },
      collection: createCollection([]),
      frontendDir: FRONTEND_DIR,
      logger: { error() {} },
    });
    ({ server, baseUrl } = await startServer(app));
  });

  after(async () => {
    await stopServer(server);
  });

  it("returns 500 when Kafka rejects the message", async () => {
    const response = await fetch(`${baseUrl}/api/registrations`, {
      method: "POST",
    });

    assert.equal(response.status, 500);
    assert.deepEqual(await response.json(), { error: "Request failed" });
  });
});
