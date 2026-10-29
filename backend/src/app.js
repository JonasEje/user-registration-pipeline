import express from "express";

import { buildRegistration, loadDailyCounts } from "./registrations.js";

export function createApp({
  publisher,
  collection,
  frontendDir,
  logger = console,
}) {
  const app = express();
  app.use(express.json());

  app.get("/api/health", (_request, response) => {
    response.json({ status: "ok" });
  });

  app.get("/api/registrations/stats", async (_request, response, next) => {
    try {
      const days = await loadDailyCounts(collection);
      response.json({ days });
    } catch (error) {
      next(error);
    }
  });

  app.post("/api/registrations", async (_request, response, next) => {
    const registration = buildRegistration();
    try {
      await publisher.publish(registration);
      response.status(201).json(registration);
    } catch (error) {
      next(error);
    }
  });

  app.use(express.static(frontendDir));

  app.use("/api", (_request, response) => {
    response.status(404).json({ error: "Not found" });
  });

  app.use((error, _request, response, _next) => {
    logger.error("Request failed:", error.message);
    response.status(500).json({ error: "Request failed" });
  });

  return app;
}
