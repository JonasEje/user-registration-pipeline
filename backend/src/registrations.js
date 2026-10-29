import { randomUUID } from "node:crypto";

const DAY_FORMAT = "%Y-%m-%d";

export function buildRegistration(now = new Date()) {
  return {
    _id: randomUUID(),
    registered_at: now.toISOString(),
  };
}

export function buildDailyCountsPipeline() {
  return [
    { $match: { registered_at: { $type: "string" } } },
    {
      $addFields: {
        registration_date: {
          $dateToString: {
            date: {
              $dateFromString: {
                dateString: "$registered_at",
                onError: null,
                onNull: null,
              },
            },
            format: DAY_FORMAT,
            timezone: "UTC",
            onNull: null,
          },
        },
      },
    },
    { $match: { registration_date: { $ne: null } } },
    { $group: { _id: "$registration_date", new_users: { $sum: 1 } } },
    { $sort: { _id: 1 } },
  ];
}

export async function loadDailyCounts(collection) {
  const rows = await collection
    .aggregate(buildDailyCountsPipeline())
    .toArray();

  return rows.map((row) => ({
    registration_date: row._id,
    new_users: row.new_users,
  }));
}
