#!/usr/bin/env bun
/**
 * Bluesky Jetstream Data Collector
 * Connects to Bluesky's Jetstream WebSocket and collects posts for testing.
 *
 * Usage: bun run collect-bluesky-posts.ts [count]
 * Default: 10000 posts
 */

const JETSTREAM_URL = "wss://jetstream2.us-west.bsky.network/subscribe";
const TARGET_COUNT = parseInt(process.argv[2] || "10000", 10);
const OUTPUT_FILE = "./bluesky-posts.json";

interface JetstreamCommit {
  did: string;
  time_us: number;
  kind: "commit";
  commit: {
    rev: string;
    operation: "create" | "update" | "delete";
    collection: string;
    rkey: string;
    record?: {
      $type: string;
      text?: string;
      createdAt?: string;
      langs?: string[];
      [key: string]: unknown;
    };
    cid?: string;
  };
}

interface CollectedPost {
  id: string;
  did: string;
  rkey: string;
  text: string;
  created_at: string;
  langs: string[];
  collected_at: number;
}

const posts: CollectedPost[] = [];
let messageCount = 0;

console.log(`Collecting ${TARGET_COUNT} posts from Bluesky Jetstream...`);
console.log(`Connecting to ${JETSTREAM_URL}`);

const url = `${JETSTREAM_URL}?wantedCollections=app.bsky.feed.post`;

const ws = new WebSocket(url);

ws.onopen = () => {
  console.log("Connected to Jetstream");
};

ws.onmessage = (event) => {
  messageCount++;

  try {
    const data = JSON.parse(event.data as string) as JetstreamCommit;

    // Only collect create operations with text content
    if (
      data.kind === "commit" &&
      data.commit.operation === "create" &&
      data.commit.record?.text
    ) {
      const post: CollectedPost = {
        id: `${data.did}:${data.commit.rkey}`,
        did: data.did,
        rkey: data.commit.rkey,
        text: data.commit.record.text,
        created_at: data.commit.record.createdAt || new Date().toISOString(),
        langs: data.commit.record.langs || [],
        collected_at: data.time_us,
      };

      posts.push(post);

      if (posts.length % 1000 === 0) {
        console.log(`Collected ${posts.length}/${TARGET_COUNT} posts (${messageCount} messages processed)`);
      }

      if (posts.length >= TARGET_COUNT) {
        console.log(`\nReached target of ${TARGET_COUNT} posts!`);
        ws.close();
      }
    }
  } catch (e) {
    // Skip malformed messages
  }
};

ws.onerror = (error) => {
  console.error("WebSocket error:", error);
};

ws.onclose = async () => {
  console.log(`\nConnection closed. Collected ${posts.length} posts from ${messageCount} messages.`);

  // Save to file
  const output = {
    collected_at: new Date().toISOString(),
    count: posts.length,
    posts: posts,
  };

  await Bun.write(OUTPUT_FILE, JSON.stringify(output, null, 2));
  console.log(`Saved to ${OUTPUT_FILE}`);

  process.exit(0);
};

// Handle graceful shutdown
process.on("SIGINT", () => {
  console.log("\nInterrupted. Saving collected posts...");
  ws.close();
});
