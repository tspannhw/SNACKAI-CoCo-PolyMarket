import snowflake from 'snowflake-sdk';
import fs from 'fs';

let connectionPool: snowflake.Connection | null = null;

function isConfigured(): boolean {
  return !!(
    process.env.SNOWFLAKE_ACCOUNT &&
    process.env.SNOWFLAKE_USER &&
    (process.env.SNOWFLAKE_PASSWORD || process.env.SNOWFLAKE_PAT || process.env.SNOWFLAKE_PRIVATE_KEY_PATH)
  );
}

function getConnectionConfig(): snowflake.ConnectionOptions {
  const config: snowflake.ConnectionOptions = {
    account: process.env.SNOWFLAKE_ACCOUNT || '',
    username: process.env.SNOWFLAKE_USER || '',
    database: process.env.SNOWFLAKE_DATABASE || 'POLYMARKET',
    schema: process.env.SNOWFLAKE_SCHEMA || 'STREAMING',
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'INGEST',
    role: process.env.SNOWFLAKE_ROLE || 'ACCOUNTADMIN',
  };

  if (process.env.SNOWFLAKE_PAT) {
    config.token = process.env.SNOWFLAKE_PAT;
    config.authenticator = 'PROGRAMMATIC_ACCESS_TOKEN';
  } else if (process.env.SNOWFLAKE_PRIVATE_KEY_PATH) {
    const keyPath = process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
    const keyContent = fs.readFileSync(keyPath, 'utf8');
    config.privateKey = keyContent;
    config.authenticator = 'SNOWFLAKE_JWT';
  } else if (process.env.SNOWFLAKE_PASSWORD) {
    config.password = process.env.SNOWFLAKE_PASSWORD;
  }

  return config;
}

/** Clear cached connection so the next call creates a fresh one. */
function resetConnection(): void {
  if (connectionPool) {
    try {
      connectionPool.destroy(() => {});
    } catch {
      // ignore destroy errors on stale connections
    }
  }
  connectionPool = null;
}

async function getConnection(): Promise<snowflake.Connection> {
  if (connectionPool) {
    return connectionPool;
  }

  if (!isConfigured()) {
    throw new Error(
      'Snowflake not configured. Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD or SNOWFLAKE_PAT or SNOWFLAKE_PRIVATE_KEY_PATH in .env.local'
    );
  }

  const config = getConnectionConfig();
  const conn = snowflake.createConnection(config);

  return new Promise((resolve, reject) => {
    conn.connect((err) => {
      if (err) {
        console.error('Snowflake connection error:', err.message);
        reject(err);
      } else {
        connectionPool = conn;
        resolve(conn);
      }
    });
  });
}

/**
 * Returns true if the error indicates a broken/stale connection that
 * may succeed on reconnect.
 */
function isConnectionError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const msg = err.message.toLowerCase();
  return (
    msg.includes('network') ||
    msg.includes('socket') ||
    msg.includes('timeout') ||
    msg.includes('connection') ||
    msg.includes('gone away') ||
    msg.includes('econnreset') ||
    msg.includes('not connected')
  );
}

export async function querySnowflake<T = Record<string, unknown>>(
  sql: string,
  binds?: snowflake.Binds
): Promise<T[]> {
  const conn = await getConnection();

  try {
    return await new Promise<T[]>((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        binds: binds,
        complete: (err, stmt, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve((rows || []) as T[]);
          }
        },
      });
    });
  } catch (err) {
    // If the error looks like a stale connection, reconnect and retry once
    if (isConnectionError(err)) {
      console.warn('Snowflake connection error detected, reconnecting...');
      resetConnection();
      const freshConn = await getConnection();
      return new Promise<T[]>((resolve, reject) => {
        freshConn.execute({
          sqlText: sql,
          binds: binds,
          complete: (retryErr, stmt, rows) => {
            if (retryErr) {
              console.error('Query error after reconnect:', (retryErr as Error).message);
              reject(retryErr);
            } else {
              resolve((rows || []) as T[]);
            }
          },
        });
      });
    }
    console.error('Query error:', (err as Error).message);
    throw err;
  }
}

export async function testConnection(): Promise<boolean> {
  try {
    const rows = await querySnowflake('SELECT CURRENT_TIMESTAMP() AS ts');
    return rows.length > 0;
  } catch {
    return false;
  }
}
