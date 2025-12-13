import { connect, StringCodec, consumerOpts } from "nats";
import mysql from "mysql2/promise";

const MYSQL_HOST = process.env.MYSQL_HOST || "localhost";
const MYSQL_PORT = parseInt(process.env.MYSQL_PORT || "3306");
const NATS_URL = process.env.NATS_URL || "nats://localhost:4222";

const wsClients = new Set<any>();
let db: mysql.Pool;

async function initDB() {
  console.log(`Connecting to MySQL at ${MYSQL_HOST}:${MYSQL_PORT}...`);
  for (let i = 0; i < 30; i++) {
    try {
      db = mysql.createPool({
        host: MYSQL_HOST,
        port: MYSQL_PORT,
        user: "root",
        connectionLimit: 10,
      });
      await db.query("SELECT 1");
      break;
    } catch {
      console.log(`Waiting for MySQL... (${30 - i} retries left)`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  await db.query("CREATE DATABASE IF NOT EXISTS app");
  await db.query("USE app");
  await db.query(
    `CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY, text TEXT NOT NULL, done INTEGER DEFAULT 0)`,
  );
  console.log("Database ready!");
}

async function startNats() {
  console.log(`Connecting to NATS at ${NATS_URL}...`);

  let nc;
  for (let i = 0; i < 30; i++) {
    try {
      nc = await connect({ servers: NATS_URL });
      console.log("NATS connected");
      break;
    } catch {
      console.log(`Waiting for NATS... (${30 - i} retries left)`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  if (!nc) {
    console.error("Failed to connect to NATS");
    return;
  }

  const js = nc.jetstream();
  const sc = StringCodec();

  // Poll for CDC streams
  (async () => {
    const subscribed = new Set<string>();

    while (true) {
      try {
        const jsm = await nc.jetstreamManager();
        const streams = await jsm.streams.list().next();

        for (const stream of streams) {
          const streamName = stream.config.name;
          if (streamName.startsWith("cdc_") && !subscribed.has(streamName)) {
            console.log(`Found CDC stream: ${streamName}`);
            subscribed.add(streamName);

            // Subscribe to this stream
            const subject = stream.config.subjects?.[0];
            if (subject) {
              try {
                const consumer = await js.consumers.get(streamName, {
                  filterSubjects: [subject],
                });

                // Consume messages
                (async () => {
                  const messages = await consumer.consume();
                  for await (const msg of messages) {
                    const raw = sc.decode(msg.data);
                    let op = null;
                    try {
                      op = JSON.parse(raw).payload?.op;
                    } catch {}
                    const payload = JSON.stringify({ op, raw });
                    console.log(
                      `[CDC] ${subject}: ${raw.substring(0, 100)}...`,
                    );
                    wsClients.forEach((ws) => {
                      try {
                        ws.send(payload);
                      } catch {
                        wsClients.delete(ws);
                      }
                    });
                    msg.ack();
                  }
                })();
              } catch (e) {
                console.log(`Error subscribing to ${streamName}:`, e);
              }
            }
          }
        }
      } catch (e) {
        // Stream listing may fail if JetStream not ready
      }
      await new Promise((r) => setTimeout(r, 2000));
    }
  })();

  console.log("NATS stream poller started");
}

const server = Bun.serve({
  port: 3000,
  async fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === "/ws") {
      if (server.upgrade(req)) return;
      return new Response("Upgrade failed", { status: 400 });
    }

    if (url.pathname === "/" || url.pathname === "/index.html") {
      return new Response(Bun.file("./public/index.html"));
    }

    await db.query("USE app");

    if (url.pathname === "/api/todos" && req.method === "GET") {
      const [rows] = (await db.query(
        "SELECT id, text, done FROM todos ORDER BY id DESC",
      )) as any;
      const todos = rows.map((r: any) => ({
        ...r,
        done: Number(r.done) === 1,
      }));
      return Response.json(todos);
    }

    if (url.pathname === "/api/todos" && req.method === "POST") {
      const { text } = await req.json();
      const [r] = (await db.query(
        "SELECT COALESCE(MAX(id),0)+1 as next FROM todos",
      )) as any;
      await db.query("INSERT INTO todos (id, text, done) VALUES (?, ?, 0)", [
        r[0].next,
        text,
      ]);
      return Response.json({ id: r[0].next });
    }

    if (url.pathname === "/api/todos" && req.method === "DELETE") {
      await db.query("DELETE FROM todos");
      return Response.json({ ok: true });
    }

    const match = url.pathname.match(/^\/api\/todos\/(\d+)$/);
    if (match) {
      const id = parseInt(match[1]);
      if (req.method === "PATCH") {
        const body = await req.json();
        if (body.text !== undefined)
          await db.query("UPDATE todos SET text=? WHERE id=?", [body.text, id]);
        if (body.done !== undefined)
          await db.query("UPDATE todos SET done=? WHERE id=?", [
            body.done ? 1 : 0,
            id,
          ]);
        return Response.json({ ok: true });
      }
      if (req.method === "DELETE") {
        await db.query("DELETE FROM todos WHERE id=?", [id]);
        return Response.json({ ok: true });
      }
    }

    return new Response("Not Found", { status: 404 });
  },
  websocket: {
    open(ws) {
      wsClients.add(ws);
      console.log("WebSocket connected");
    },
    close(ws) {
      wsClients.delete(ws);
    },
    message() {},
  },
});

(async () => {
  await initDB();
  await startNats();
  console.log(`\n=== TODO App with CDC (NATS) ===\nhttp://localhost:3000\n`);
})();
