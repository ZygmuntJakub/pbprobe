const { Client } = require('pg');
const BatchQuery = require('pg-batch-query').default;

const PORT = 5433;
const SETUP_PORT = 5432;
const RUNS = 5;
const SIZES = [10, 50, 100, 500, 1000, 5000];

const connConfig = (port) => ({
  host: 'localhost',
  port,
  user: 'postgres',
  password: 'postgres',
  database: 'testdb',
});

async function setup() {
  const client = new Client(connConfig(SETUP_PORT));
  await client.connect();

  await client.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  const { rows } = await client.query('SELECT COUNT(*)::int AS cnt FROM users');
  if (rows[0].cnt === 0) {
    const values = [];
    const params = [];
    for (let i = 0; i < 1000; i++) {
      const off = i * 2;
      values.push(`($${off + 1}, $${off + 2})`);
      params.push(`user_${i + 1}`, `user_${i + 1}@example.com`);
    }
    await client.query(
      `INSERT INTO users (name, email) VALUES ${values.join(', ')}`,
      params,
    );
    console.log('Seeded 1000 rows into users table.');
  }

  await client.query(`
    CREATE TABLE IF NOT EXISTS batch_test (
      id SERIAL PRIMARY KEY,
      val TEXT NOT NULL
    )
  `);

  await client.end();
}

function randomIds(n) {
  return Array.from({ length: n }, () => Math.floor(Math.random() * 1000) + 1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// --- Strategies ---

const STRATEGIES = {
  sequential: {
    description: 'await loop — one query at a time (simple protocol)',
    reads: async (client, ids, query) => {
      for (const id of ids) {
        await client.query(query, [id]);
      }
    },
    writes: async (client, n) => {
      for (let i = 0; i < n; i++) {
        await client.query(
          'INSERT INTO batch_test (val) VALUES ($1) RETURNING id',
          [`item_${i}`],
        );
      }
    },
    updates: async (client, ids) => {
      for (const id of ids) {
        await client.query(
          'UPDATE users SET name = $1 WHERE id = $2 RETURNING id',
          [`updated_${id}`, id],
        );
      }
    },
  },

  batch: {
    description: 'pg-batch-query — pipelined Parse/Bind/Execute (extended protocol)',
    reads: async (client, ids, query) => {
      const b = new BatchQuery({ text: query, values: ids.map((id) => [id]) });
      await client.query(b).execute();
    },
    writes: async (client, n) => {
      const b = new BatchQuery({
        text: 'INSERT INTO batch_test (val) VALUES ($1) RETURNING id',
        values: Array.from({ length: n }, (_, i) => [`item_${i}`]),
      });
      await client.query(b).execute();
    },
    updates: async (client, ids) => {
      const b = new BatchQuery({
        text: 'UPDATE users SET name = $1 WHERE id = $2 RETURNING id',
        values: ids.map((id) => [`updated_${id}`, id]),
      });
      await client.query(b).execute();
    },
  },

  any: {
    description: 'ANY($1::int[]) — single query with array parameter (reads only)',
    reads: async (client, ids) => {
      await client.query(
        'SELECT id FROM users WHERE id = ANY($1::int[]) ORDER BY id',
        [ids],
      );
    },
    writes: null,
    updates: null,
  },

};

const RUN_TIMEOUT_MS = 10_000;

// --- Runner ---

async function run(label, fn, warmupFn) {
  console.log(`    ${label} ...`);
  try {
    for (let i = 0; i < RUNS; i++) {
      const client = new Client(connConfig(PORT));
      await client.connect();

      if (warmupFn) {
        await warmupFn(client);
      }

      await Promise.race([
        fn(client).then(() => client.end()),
        sleep(RUN_TIMEOUT_MS).then(() => {
          client.end().catch(() => {});
          throw new Error('timeout');
        }),
      ]);
    }
    console.log(`    ${label} done`);
  } catch (err) {
    console.log(`    ${label} FAILED (${err.message})`);
  }
}

async function runReads(strategy) {
  const queries = {
    narrow: 'SELECT id FROM users WHERE id = $1',
    wide: 'SELECT * FROM users WHERE id = $1',
    fat: "SELECT id, repeat(name, 100) AS padded FROM users WHERE id = $1",
  };

  console.log('\n  reads — scaling (narrow SELECT)');
  for (const n of SIZES) {
    if (strategy.reads === null) {
      console.log(`    N=${n} — not supported`);
      continue;
    }
    const ids = randomIds(n);
    const warmup = async (c) => {
      for (let i = 0; i < 10; i++) await c.query('SELECT 1');
    };
    await run(`N=${n}`, (c) => strategy.reads(c, ids, queries.narrow), warmup);
    await sleep(300);
  }

  console.log('\n  reads — payload (N=500)');
  for (const [label, query] of Object.entries(queries)) {
    if (strategy.reads === null) {
      console.log(`    ${label} — not supported`);
      continue;
    }
    const ids = randomIds(500);
    const warmup = async (c) => {
      for (let i = 0; i < 10; i++) await c.query(query, [1]);
    };
    await run(label, (c) => strategy.reads(c, ids, query), warmup);
    await sleep(300);
  }
}

async function runWrites(strategy) {
  const sizes = [10, 50, 100, 500, 1000];

  console.log('\n  writes — INSERT scaling');
  for (const n of sizes) {
    if (strategy.writes === null) {
      console.log(`    N=${n} — not supported`);
      continue;
    }
    const warmup = async (c) => {
      await c.query('DELETE FROM batch_test');
    };
    await run(`N=${n}`, async (c) => {
      await c.query('DELETE FROM batch_test');
      await strategy.writes(c, n);
    }, warmup);
    await sleep(300);
  }
}

async function runUpdates(strategy) {
  const sizes = [10, 50, 100, 500];

  console.log('\n  updates — UPDATE scaling');
  for (const n of sizes) {
    if (strategy.updates === null) {
      console.log(`    N=${n} — not supported`);
      continue;
    }
    const ids = randomIds(n);
    const warmup = async (c) => {
      await c.query('SELECT 1');
    };
    await run(`N=${n}`, (c) => strategy.updates(c, ids), warmup);
    await sleep(300);
  }
}

// --- Main ---

async function main() {
  const arg = process.argv[2];
  const strategyNames = Object.keys(STRATEGIES);

  if (arg === '--help' || arg === '-h') {
    console.log(`Usage: node batch.js <${strategyNames.join('|')}>`);
    console.log('  Runs all workloads with the chosen strategy through dbprobe (:5433).');
    console.log('  No argument runs all strategies.\n');
    for (const [name, s] of Object.entries(STRATEGIES)) {
      console.log(`  ${name.padEnd(12)} ${s.description}`);
    }
    process.exit(0);
  }

  if (arg && !STRATEGIES[arg]) {
    console.error(
      `Unknown strategy: ${arg}\nAvailable: ${strategyNames.join(', ')}`,
    );
    process.exit(1);
  }

  await setup();

  const selected = arg
    ? { [arg]: STRATEGIES[arg] }
    : STRATEGIES;

  const t0 = performance.now();

  for (const [name, strategy] of Object.entries(selected)) {
    console.log(`\n=== ${name} — ${strategy.description} ===`);

    await runReads(strategy);
    await runWrites(strategy);
    await runUpdates(strategy);

    console.log(`\n=== ${name} done ===`);
    await sleep(500);
  }

  const elapsed = ((performance.now() - t0) / 1000).toFixed(1);
  console.log(`\nAll done in ${elapsed}s.`);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
