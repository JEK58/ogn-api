import knex from "knex";
import type { SenderPosition } from "./types/common";
import { Hono } from "hono";
import type { QueryResult } from "pg";

const app = new Hono();

const db = knex({
  client: "pg",
  connection: process.env.DATABASE_URL,
  searchPath: ["knex", "public"],
});

const INACTIVE_AFTER = 30; // 30 minutes

if (!Bun.env.API_KEY || Bun.env.API_KEY === "")
  throw new Error("No API Key set");

app.get("api/flarm/:ids", async (c) => {
  if (Bun.env.API_KEY !== c.req.header("API-Key")) {
    c.status(401);
    return c.json({ error: "Unauthorized / Wrong API Key" });
  }
  console.time("Execution Time"); // Start timer

  const ids = c.req.param("ids").split(",");
  console.log("🚀 ~ ids:", ids, new Date().toISOString());

  try {
    // First find all IDs that reported a position less than INACTIVE_AFTER minutes ago
    const recentlySeen = await db("senders")
      .select("address")
      .whereIn("address", [...ids])
      .groupBy("address")
      .havingRaw(
        `MAX(lastseen) > NOW() - INTERVAL '${INACTIVE_AFTER} minutes'`
      );

    const recentlySeenIds = recentlySeen.map(({ address }) => address);
    console.log("🚀 ~ recentlySeen:", recentlySeenIds);

    // Then fetch all positions of those IDs from today
    const fixes = await db<QueryResult<SenderPosition>>("sender_positions")
      .select(
        "address",
        "altitude",
        db.raw("EXTRACT(EPOCH FROM timestamp) as timestamp"),
        db.raw("ST_X(location) as lon, ST_Y(location) as lat")
      )
      .where(db.raw("DATE(timestamp) = CURRENT_DATE"))
      .whereIn("address", [...recentlySeenIds])
      .whereRaw(
        `
        ST_Within("location", ST_MakePolygon (ST_GeomFromText('LINESTRING(
          5.918960 51.869971,
          11.653823 52.059508,
          10.291518 48.712994, 
          7.819594 48.480487, 
          5.710219 49.504086,
          5.918960 51.869971        
        )', 4326)))
      `
      )
      .orderBy("timestamp", "asc");

    // @ts-expect-error
    const fixesGrouped = Object.groupBy(fixes, ({ address }) => address);
    const activeIds = Object.keys(fixesGrouped);

    // Find the track distance for every ID
    const distances = await db<QueryResult<SenderPosition>>("sender_positions")
      .select(
        "address",
        db.raw(`
      ST_Length(
        ST_Transform(ST_MakeLine(location ORDER BY timestamp ASC), 26986)
        ) / 1000 as DISTANCE
        `)
      )
      .where(db.raw("DATE(timestamp) = CURRENT_DATE"))
      .whereIn("address", [...activeIds])
      .groupBy("address");

    const result = distances.reduce((obj, item) => {
      obj[item.address] = item.distance;
      return obj;
    }, {});

    console.timeEnd("Execution Time");
    return c.json({ tracks: fixesGrouped, distances: result });
  } catch (error) {
    console.log("🚀 ~ error:", error);
    c.status(500);
    return c.json({ error: "Something went wrong" });
  }
});

export default {
  port: 4000,
  fetch: app.fetch,
};
