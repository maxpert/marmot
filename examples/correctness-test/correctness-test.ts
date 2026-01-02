#!/usr/bin/env bun
/**
 * Marmot Correctness Verification Test
 *
 * Tests distributed SQLite consistency by:
 * 1. Starting a 3-node cluster + 1 replica
 * 2. Loading test data into both cluster and local SQLite
 * 3. Performing random insert/update/delete operations
 * 4. Verifying data consistency across all nodes
 * 5. Optional: Running high-load stress test
 *
 * Usage:
 *   bun run correctness-test.ts              # Run basic correctness tests
 *   bun run correctness-test.ts --load       # Run with high-load simulation
 *   bun run correctness-test.ts --load-only  # Only run load test (skip basic tests)
 */

import { Database } from "bun:sqlite";
import { spawn, type Subprocess } from "bun";
import mysql from "mysql2/promise";

// Configuration
const CONFIG = {
  dataFile: "./bluesky-posts.json",
  localDbPath: "/tmp/marmot-correctness/local.db",
  clusterDataDir: "/tmp/marmot-correctness",
  nodes: [
    { id: 1, grpcPort: 9081, mysqlPort: 4307 },
    { id: 2, grpcPort: 9082, mysqlPort: 4308 },
    { id: 3, grpcPort: 9083, mysqlPort: 4309 },
  ],
  replica: { id: 100, grpcPort: 9091, mysqlPort: 4317 },
  startupWaitMs: 5000,
  replicationWaitMs: 5000,
  loadTestDurationSec: 60,
  loadTestOpsPerSec: 100,
};

// Types
interface CollectedPost {
  id: string;
  did: string;
  rkey: string;
  text: string;
  created_at: string;
  langs: string[];
  collected_at: number;
}

interface TestData {
  collected_at: string;
  count: number;
  posts: CollectedPost[];
}

// Globals
let localDb: Database;
let processes: Subprocess[] = [];
let testData: TestData;

// Utility functions
function log(msg: string) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function execCommand(cmd: string): Promise<string> {
  const proc = spawn(["bash", "-c", cmd], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const output = await new Response(proc.stdout).text();
  await proc.exited;
  return output.trim();
}

// Cluster management
async function buildMarmot(): Promise<string> {
  log("Building marmot-v2...");
  const repoRoot = await execCommand("cd ../.. && pwd");
  await execCommand(`cd ${repoRoot} && go build -tags sqlite_preupdate_hook -o marmot-v2 .`);
  return `${repoRoot}/marmot-v2`;
}

function generateNodeConfig(nodeId: number, grpcPort: number, mysqlPort: number, clusterSecret: string): string {
  const seeds = CONFIG.nodes
    .filter((n) => n.id !== nodeId)
    .map((n) => `"localhost:${n.grpcPort}"`)
    .join(", ");

  return `
node_id = ${nodeId}
data_dir = "${CONFIG.clusterDataDir}/node-${nodeId}"

[cluster]
grpc_bind_address = "0.0.0.0"
grpc_advertise_address = "localhost:${grpcPort}"
grpc_port = ${grpcPort}
seed_nodes = [${seeds}]
cluster_secret = "${clusterSecret}"
gossip_interval_ms = 500
gossip_fanout = 2
suspect_timeout_ms = 3000
dead_timeout_ms = 6000

[replica]
secret = "${clusterSecret}"

[transaction]
heartbeat_timeout_seconds = 10
conflict_window_seconds = 10
lock_wait_timeout_seconds = 50

[replication]
default_write_consistency = "QUORUM"
default_read_consistency = "LOCAL_ONE"
write_timeout_ms = 30000
read_timeout_ms = 2000
enable_anti_entropy = true
anti_entropy_interval_seconds = 30

[mysql]
enabled = true
bind_address = "0.0.0.0"
port = ${mysqlPort}
max_connections = 100

[metastore]
cache_size_mb = 64
memtable_size_mb = 64

[logging]
verbose = false
format = "json"

[prometheus]
enabled = true
`;
}

function generateReplicaConfig(replicaId: number, grpcPort: number, mysqlPort: number, clusterSecret: string): string {
  const followAddrs = CONFIG.nodes.map((n) => `"localhost:${n.grpcPort}"`).join(", ");

  return `
node_id = ${replicaId}
data_dir = "${CONFIG.clusterDataDir}/replica-${replicaId}"

[cluster]
grpc_bind_address = "0.0.0.0"
grpc_port = ${grpcPort}
grpc_advertise_address = "localhost:${grpcPort}"

[replica]
enabled = true
follow_addresses = [${followAddrs}]
secret = "${clusterSecret}"
discovery_interval_seconds = 10
failover_timeout_seconds = 30
reconnect_interval_seconds = 2
reconnect_max_backoff_seconds = 10
initial_sync_timeout_minutes = 5

[mysql]
enabled = true
bind_address = "0.0.0.0"
port = ${mysqlPort}

[metastore]
cache_size_mb = 64
memtable_size_mb = 64

[logging]
verbose = false
format = "json"

[prometheus]
enabled = true
`;
}

async function startCluster(binaryPath: string): Promise<void> {
  log("Starting cluster...");

  // Clean up
  await execCommand("pkill -f 'marmot-v2.*correctness' || true");
  await sleep(1000);
  await execCommand(`rm -rf ${CONFIG.clusterDataDir}`);
  await execCommand(`mkdir -p ${CONFIG.clusterDataDir}`);

  const clusterSecret = crypto.randomUUID().replace(/-/g, "");

  // Start primary nodes
  for (const node of CONFIG.nodes) {
    const configPath = `${CONFIG.clusterDataDir}/node-${node.id}.toml`;
    const config = generateNodeConfig(node.id, node.grpcPort, node.mysqlPort, clusterSecret);
    await Bun.write(configPath, config);
    await execCommand(`mkdir -p ${CONFIG.clusterDataDir}/node-${node.id}`);

    const logPath = `${CONFIG.clusterDataDir}/node-${node.id}/marmot.log`;
    const proc = spawn([binaryPath, "--config", configPath], {
      stdout: Bun.file(logPath),
      stderr: Bun.file(logPath),
    });
    processes.push(proc);
    log(`Started Node ${node.id} (MySQL: ${node.mysqlPort}, gRPC: ${node.grpcPort})`);
    await sleep(1000);
  }

  // Wait for cluster to form
  log(`Waiting ${CONFIG.startupWaitMs}ms for cluster formation...`);
  await sleep(CONFIG.startupWaitMs);

  // Start replica
  const replicaConfigPath = `${CONFIG.clusterDataDir}/replica-${CONFIG.replica.id}.toml`;
  const replicaConfig = generateReplicaConfig(
    CONFIG.replica.id,
    CONFIG.replica.grpcPort,
    CONFIG.replica.mysqlPort,
    clusterSecret
  );
  await Bun.write(replicaConfigPath, replicaConfig);
  await execCommand(`mkdir -p ${CONFIG.clusterDataDir}/replica-${CONFIG.replica.id}`);

  const replicaLogPath = `${CONFIG.clusterDataDir}/replica-${CONFIG.replica.id}/marmot.log`;
  const replicaProc = spawn([binaryPath, "--config", replicaConfigPath], {
    stdout: Bun.file(replicaLogPath),
    stderr: Bun.file(replicaLogPath),
  });
  processes.push(replicaProc);
  log(`Started Replica ${CONFIG.replica.id} (MySQL: ${CONFIG.replica.mysqlPort})`);

  // Wait for replica to sync
  log(`Waiting ${CONFIG.startupWaitMs}ms for replica sync...`);
  await sleep(CONFIG.startupWaitMs);
}

async function stopCluster(): Promise<void> {
  log("Stopping cluster...");
  for (const proc of processes) {
    proc.kill();
  }
  await sleep(1000);
  await execCommand("pkill -f 'marmot-v2.*correctness' || true");
  processes = [];
}

// Database operations
async function getMysqlConnection(port: number): Promise<mysql.Connection> {
  const conn = await mysql.createConnection({
    host: "127.0.0.1",
    port: port,
    user: "root",
    connectTimeout: 10000,
    namedPlaceholders: false,
  });
  await conn.query("USE marmot");
  return conn;
}

async function setupSchema(): Promise<void> {
  log("Setting up schema...");

  // Setup local SQLite
  await execCommand(`mkdir -p $(dirname ${CONFIG.localDbPath})`);
  localDb = new Database(CONFIG.localDbPath, { create: true });
  localDb.run(`
    CREATE TABLE IF NOT EXISTS posts (
      id TEXT PRIMARY KEY,
      did TEXT NOT NULL,
      rkey TEXT NOT NULL,
      text TEXT NOT NULL,
      created_at TEXT NOT NULL,
      langs TEXT,
      collected_at INTEGER
    )
  `);

  // Setup cluster schema via MySQL (getMysqlConnection already does USE marmot)
  const conn = await getMysqlConnection(CONFIG.nodes[0].mysqlPort);
  await conn.query(`
    CREATE TABLE IF NOT EXISTS posts (
      id TEXT PRIMARY KEY,
      did TEXT NOT NULL,
      rkey TEXT NOT NULL,
      text TEXT NOT NULL,
      created_at TEXT NOT NULL,
      langs TEXT,
      collected_at INTEGER
    )
  `);
  await conn.end();
  log("Schema created");
}

async function loadTestData(): Promise<void> {
  log(`Loading test data from ${CONFIG.dataFile}...`);

  const file = Bun.file(CONFIG.dataFile);
  if (!(await file.exists())) {
    throw new Error(`Test data file not found: ${CONFIG.dataFile}. Run 'bun run collect' first.`);
  }

  testData = await file.json();
  log(`Loaded ${testData.count} posts from ${testData.collected_at}`);
}

async function insertPosts(posts: CollectedPost[], target: "local" | "cluster" | "both"): Promise<void> {
  if (target === "local" || target === "both") {
    const stmt = localDb.prepare(`
      INSERT OR REPLACE INTO posts (id, did, rkey, text, created_at, langs, collected_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    for (const post of posts) {
      stmt.run(post.id, post.did, post.rkey, post.text, post.created_at, JSON.stringify(post.langs), post.collected_at);
    }
  }

  if (target === "cluster" || target === "both") {
    const conn = await getMysqlConnection(CONFIG.nodes[0].mysqlPort);
    for (const post of posts) {
      await conn.execute(
        `INSERT OR REPLACE INTO posts (id, did, rkey, text, created_at, langs, collected_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [post.id, post.did, post.rkey, post.text, post.created_at, JSON.stringify(post.langs), post.collected_at]
      );
    }
    await conn.end();
  }
}

async function deletePosts(ids: string[], target: "local" | "cluster" | "both"): Promise<void> {
  if (target === "local" || target === "both") {
    const stmt = localDb.prepare("DELETE FROM posts WHERE id = ?");
    for (const id of ids) {
      stmt.run(id);
    }
  }

  if (target === "cluster" || target === "both") {
    const conn = await getMysqlConnection(CONFIG.nodes[0].mysqlPort);
    for (const id of ids) {
      await conn.execute("DELETE FROM posts WHERE id = ?", [id]);
    }
    await conn.end();
  }
}

async function updatePosts(updates: { id: string; text: string }[], target: "local" | "cluster" | "both"): Promise<void> {
  if (target === "local" || target === "both") {
    const stmt = localDb.prepare("UPDATE posts SET text = ? WHERE id = ?");
    for (const update of updates) {
      stmt.run(update.text, update.id);
    }
  }

  if (target === "cluster" || target === "both") {
    const conn = await getMysqlConnection(CONFIG.nodes[0].mysqlPort);
    for (const update of updates) {
      await conn.execute("UPDATE posts SET text = ? WHERE id = ?", [update.text, update.id]);
    }
    await conn.end();
  }
}

// Verification
interface VerificationResult {
  success: boolean;
  localCount: number;
  clusterCounts: { nodeId: number; port: number; count: number }[];
  replicaCount: number;
  errors: string[];
}

async function verifyConsistency(): Promise<VerificationResult> {
  const result: VerificationResult = {
    success: true,
    localCount: 0,
    clusterCounts: [],
    replicaCount: 0,
    errors: [],
  };

  // Get local count
  const localRow = localDb.query("SELECT COUNT(*) as cnt FROM posts").get() as { cnt: number };
  result.localCount = localRow.cnt;

  // Get cluster counts
  for (const node of CONFIG.nodes) {
    try {
      const conn = await getMysqlConnection(node.mysqlPort);
      const [rows] = await conn.query("SELECT COUNT(*) as cnt FROM posts");
      const count = Number((rows as any)[0].cnt);
      result.clusterCounts.push({ nodeId: node.id, port: node.mysqlPort, count });
      await conn.end();
    } catch (e) {
      result.errors.push(`Node ${node.id}: ${e}`);
      result.success = false;
    }
  }

  // Get replica count
  try {
    const conn = await getMysqlConnection(CONFIG.replica.mysqlPort);
    const [rows] = await conn.query("SELECT COUNT(*) as cnt FROM posts");
    result.replicaCount = Number((rows as any)[0].cnt);
    await conn.end();
  } catch (e) {
    result.errors.push(`Replica: ${e}`);
    result.success = false;
  }

  // Verify counts match
  const expectedCount = result.localCount;
  for (const nodeCount of result.clusterCounts) {
    if (nodeCount.count !== expectedCount) {
      result.errors.push(`Node ${nodeCount.nodeId} count mismatch: expected ${expectedCount}, got ${nodeCount.count}`);
      result.success = false;
    }
  }

  if (result.replicaCount !== expectedCount) {
    result.errors.push(`Replica count mismatch: expected ${expectedCount}, got ${result.replicaCount}`);
    result.success = false;
  }

  return result;
}

async function verifyDataIntegrity(sampleSize: number = 100): Promise<{ success: boolean; errors: string[] }> {
  const errors: string[] = [];

  // Get sample IDs from local
  const localPosts = localDb
    .query("SELECT id, text FROM posts ORDER BY RANDOM() LIMIT ?")
    .all(sampleSize) as { id: string; text: string }[];

  // Verify each sample exists in cluster
  const conn = await getMysqlConnection(CONFIG.nodes[0].mysqlPort);
  for (const post of localPosts) {
    const [rows] = await conn.execute("SELECT text FROM posts WHERE id = ?", [post.id]);
    const clusterRows = rows as any[];
    if (clusterRows.length === 0) {
      errors.push(`Post ${post.id} missing in cluster`);
    } else if (clusterRows[0].text !== post.text) {
      const localText = post.text.substring(0, 50);
      const clusterText = (clusterRows[0].text as string).substring(0, 50);
      errors.push(`Post ${post.id} text mismatch: local="${localText}..." cluster="${clusterText}..."`);
    }
  }
  await conn.end();

  return { success: errors.length === 0, errors };
}

// Test phases
async function runBasicTests(): Promise<boolean> {
  log("=== Phase 1: Initial Load ===");
  const batchSize = 1000;
  const batches = Math.ceil(testData.posts.length / batchSize);

  for (let i = 0; i < batches; i++) {
    const start = i * batchSize;
    const end = Math.min(start + batchSize, testData.posts.length);
    const batch = testData.posts.slice(start, end);
    await insertPosts(batch, "both");
    log(`Loaded batch ${i + 1}/${batches} (${end} posts)`);
  }

  await sleep(CONFIG.replicationWaitMs);
  let result = await verifyConsistency();
  log(`Initial load verification: ${result.success ? "PASS" : "FAIL"}`);
  log(`  Local: ${result.localCount}, Cluster: ${result.clusterCounts.map((c) => c.count).join(",")}, Replica: ${result.replicaCount}`);
  if (!result.success) {
    result.errors.forEach((e) => log(`  ERROR: ${e}`));
    return false;
  }

  log("=== Phase 2: Random Deletes ===");
  const deleteCount = Math.floor(testData.posts.length * 0.1);
  const deleteIds = testData.posts
    .sort(() => Math.random() - 0.5)
    .slice(0, deleteCount)
    .map((p) => p.id);

  await deletePosts(deleteIds, "both");
  log(`Deleted ${deleteCount} random posts`);

  await sleep(CONFIG.replicationWaitMs);
  result = await verifyConsistency();
  log(`Delete verification: ${result.success ? "PASS" : "FAIL"}`);
  log(`  Local: ${result.localCount}, Cluster: ${result.clusterCounts.map((c) => c.count).join(",")}, Replica: ${result.replicaCount}`);
  if (!result.success) {
    result.errors.forEach((e) => log(`  ERROR: ${e}`));
    return false;
  }

  log("=== Phase 3: Random Updates ===");
  const remainingPosts = testData.posts.filter((p) => !deleteIds.includes(p.id));
  const updateCount = Math.floor(remainingPosts.length * 0.1);
  const updates = remainingPosts
    .sort(() => Math.random() - 0.5)
    .slice(0, updateCount)
    .map((p) => ({ id: p.id, text: `[UPDATED] ${p.text}` }));

  await updatePosts(updates, "both");
  log(`Updated ${updateCount} random posts`);

  await sleep(CONFIG.replicationWaitMs);
  result = await verifyConsistency();
  log(`Update verification: ${result.success ? "PASS" : "FAIL"}`);
  if (!result.success) {
    result.errors.forEach((e) => log(`  ERROR: ${e}`));
    return false;
  }

  const integrity = await verifyDataIntegrity(100);
  log(`Data integrity check: ${integrity.success ? "PASS" : "FAIL"}`);
  if (!integrity.success) {
    integrity.errors.forEach((e) => log(`  ERROR: ${e}`));
    return false;
  }

  return true;
}

async function runLoadTest(): Promise<boolean> {
  log(`=== Load Test (${CONFIG.loadTestDurationSec}s, ${CONFIG.loadTestOpsPerSec} ops/sec) ===`);

  const startTime = Date.now();
  const endTime = startTime + CONFIG.loadTestDurationSec * 1000;
  const intervalMs = 1000 / CONFIG.loadTestOpsPerSec;

  let insertCount = 0;
  let updateCount = 0;
  let deleteCount = 0;
  let errorCount = 0;
  let nextId = testData.posts.length;

  // Get current posts for update/delete operations
  const currentIds = new Set(
    (localDb.query("SELECT id FROM posts").all() as { id: string }[]).map((r) => r.id)
  );

  while (Date.now() < endTime) {
    const opStart = Date.now();

    try {
      const op = Math.random();

      if (op < 0.4) {
        // Insert (40%)
        const newPost: CollectedPost = {
          id: `load-test-${nextId++}`,
          did: `did:plc:loadtest${Math.floor(Math.random() * 1000)}`,
          rkey: `${Date.now()}`,
          text: `Load test post ${nextId} - ${crypto.randomUUID()}`,
          created_at: new Date().toISOString(),
          langs: ["en"],
          collected_at: Date.now() * 1000,
        };
        await insertPosts([newPost], "both");
        currentIds.add(newPost.id);
        insertCount++;
      } else if (op < 0.7) {
        // Update (30%)
        const ids = Array.from(currentIds);
        if (ids.length > 0) {
          const id = ids[Math.floor(Math.random() * ids.length)];
          await updatePosts([{ id, text: `Updated at ${new Date().toISOString()}` }], "both");
          updateCount++;
        }
      } else {
        // Delete (30%)
        const ids = Array.from(currentIds);
        if (ids.length > 100) {
          // Keep minimum 100 posts
          const id = ids[Math.floor(Math.random() * ids.length)];
          await deletePosts([id], "both");
          currentIds.delete(id);
          deleteCount++;
        }
      }
    } catch (e) {
      errorCount++;
    }

    // Maintain target ops/sec
    const elapsed = Date.now() - opStart;
    if (elapsed < intervalMs) {
      await sleep(intervalMs - elapsed);
    }

    // Progress update every 10 seconds
    const totalElapsed = Date.now() - startTime;
    if (totalElapsed % 10000 < intervalMs) {
      log(`Progress: ${Math.floor(totalElapsed / 1000)}s - I:${insertCount} U:${updateCount} D:${deleteCount} E:${errorCount}`);
    }
  }

  log(`Load test complete: Inserts=${insertCount}, Updates=${updateCount}, Deletes=${deleteCount}, Errors=${errorCount}`);

  // Final verification
  await sleep(CONFIG.replicationWaitMs * 2);
  const result = await verifyConsistency();
  log(`Final verification: ${result.success ? "PASS" : "FAIL"}`);
  log(`  Local: ${result.localCount}, Cluster: ${result.clusterCounts.map((c) => c.count).join(",")}, Replica: ${result.replicaCount}`);

  if (!result.success) {
    result.errors.forEach((e) => log(`  ERROR: ${e}`));
  }

  return result.success && errorCount < insertCount + updateCount + deleteCount * 0.01;
}

// Main
async function main() {
  const args = process.argv.slice(2);
  const loadMode = args.includes("--load");
  const loadOnly = args.includes("--load-only");

  log("=== Marmot Correctness Verification Test ===");

  try {
    // Load test data
    await loadTestData();

    // Build and start cluster
    const binaryPath = await buildMarmot();
    await startCluster(binaryPath);

    // Setup schema
    await setupSchema();

    let success = true;

    // Run basic tests unless load-only
    if (!loadOnly) {
      success = await runBasicTests();
      if (!success) {
        log("Basic tests FAILED");
        process.exitCode = 1;
        return;
      }
    }

    // Run load test if requested
    if (loadMode || loadOnly) {
      success = await runLoadTest();
      if (!success) {
        log("Load test FAILED");
        process.exitCode = 1;
        return;
      }
    }

    log("=== ALL TESTS PASSED ===");
  } catch (e) {
    log(`FATAL ERROR: ${e}`);
    process.exitCode = 1;
  } finally {
    await stopCluster();
    if (localDb) localDb.close();
  }
}

main();
