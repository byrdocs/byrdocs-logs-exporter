import postgres, { type Sql } from "postgres";

const BATCH_SIZE = 100;
const MAX_LIST_PAGES = 10;
const DEFAULT_IMPORT_TIMEOUT_MS = 120_000;
const DIAG_TIMEOUT_MS = 15_000;
const DEFAULT_GITHUB_WORKFLOW_REF = "main";
const IMPORT_TABLE = "_import";
const IMPORTED_AT_COLUMN = "imported_at";
const TIMESTAMP_COLUMN = "timestamp";
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const PLAIN_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;

interface Env {
  ANALYTICS_BUCKET: R2Bucket;
  EXPORTER_SITE_TOKEN?: string;
  IMPORT_TIMEOUT_MS?: string;
  GITHUB_REPOSITORY?: string;
  GITHUB_WORKFLOW_DISPATCH_TOKEN?: string;
  GITHUB_WORKFLOW_ID?: string;
  GITHUB_WORKFLOW_REF?: string;
  LOGS_DB_URL?: string;
  LOGS_DB_SSLMODE?: string;
  LOGS_DB_HOST?: string;
  LOGS_DB_PORT?: string;
  LOGS_DB_USERNAME?: string;
  LOGS_DB_PASSWORD?: string;
  LOGS_DB_DATABASE?: string;
  HYPERDRIVE?: {
    connectionString?: string;
  };
}

type LogRecord = Record<string, unknown>;
type QueryParameter = postgres.ParameterOrJSON<never>;
type PostgresConnectionOptions = NonNullable<Parameters<typeof postgres>[1]>;
type NormalizedPgType =
  | "text"
  | "bigint"
  | "double precision"
  | "boolean"
  | "jsonb"
  | "timestamptz";

interface ColumnMeta {
  column_name: string;
  data_type: string;
  udt_name: string | null;
}

interface DatasetImportResult {
  dataset: string;
  insertedRecords: number;
  deletedRecords: number;
}

interface ImportResult {
  date: string;
  totalRecords: number;
  datasets: DatasetImportResult[];
}

interface TcpProbeResult {
  mode: "object" | "string";
  ok: boolean;
  elapsedMs: number;
  address: string;
  error?: string;
}

class HttpError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = normalizePath(url.pathname);

    try {
      if (path === "/") {
        return jsonResponse({
          ok: true,
          service: "BYR Docs Logs Exporter",
          runtime: "Cloudflare Workers",
          endpoints: {
            "GET /health": "Health check",
            "GET|POST /import?date=YYYY-MM-DD&force=true&timeoutMs=120000": "Import one day of logs (date defaults to yesterday)",
            "GET /diag": "Run R2 and PostgreSQL connectivity checks",
            "GET /list?prefix=": "List objects in the R2 analytics bucket"
          }
        });
      }

      if (path === "/health") {
        return jsonResponse({
          ok: true,
          timestamp: new Date().toISOString()
        });
      }

      if (path === "/import") {
        assertMethod(request, ["GET", "POST"]);
        assertAuthorized(request, env);

        const requestId = createRequestId();
        const startedAt = Date.now();
        const date = resolveImportDate(url.searchParams.get("date"));
        const force = parseBooleanFlag(url.searchParams.get("force"));
        const timeoutMs = resolveImportTimeoutMs(
          url.searchParams.get("timeoutMs"),
          env.IMPORT_TIMEOUT_MS
        );

        console.log(
          `[${requestId}] Import request started (date=${date}, force=${force}, timeoutMs=${timeoutMs})`
        );
        const result = await withTimeout(
          importLogsForDate(env, date, force, requestId),
          timeoutMs,
          `Import request ${requestId}`
        );
        const elapsedMs = Date.now() - startedAt;
        console.log(
          `[${requestId}] Import request finished (records=${result.totalRecords}, datasets=${result.datasets.length}, elapsedMs=${elapsedMs})`
        );

        return jsonResponse({ ok: true, requestId, elapsedMs, ...result });
      }

      if (path === "/diag") {
        assertMethod(request, ["GET"]);
        assertAuthorized(request, env);

        const requestId = createRequestId();
        const prefixParam = url.searchParams.get("prefix");
        const prefix = prefixParam?.trim() ? prefixParam.trim() : "";
        const includeTcpProbe = parseBooleanFlag(url.searchParams.get("tcp"));

        const bucketProbe = await withTimeout(
          env.ANALYTICS_BUCKET.list({ limit: 5, prefix }),
          DIAG_TIMEOUT_MS,
          "R2 probe"
        );
        const dbProbe = await probeDatabase(env, requestId);
        const tcpProbe = includeTcpProbe ? await probeRawTcpConnection(env, requestId) : undefined;

        return jsonResponse({
          ok: true,
          requestId,
          r2: {
            prefix,
            objectCount: bucketProbe.objects.length,
            truncated: bucketProbe.truncated,
            sampleKeys: bucketProbe.objects.map((object) => object.key)
          },
          db: dbProbe,
          ...(tcpProbe ? { tcp: tcpProbe } : {})
        });
      }

      if (path === "/list") {
        assertMethod(request, ["GET"]);
        assertAuthorized(request, env);

        const prefixParam = url.searchParams.get("prefix");
        const prefix = prefixParam?.trim() ? prefixParam.trim() : undefined;
        const listing = await listBucketObjects(env.ANALYTICS_BUCKET, prefix);
        return jsonResponse({ ok: true, ...listing });
      }

      return jsonResponse({ ok: false, error: "Not Found" }, 404);
    } catch (error: unknown) {
      return toErrorResponse(error);
    }
  },

  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext
  ): Promise<void> {
    const date = getYesterdayUtcDate(controller.scheduledTime);
    const requestId = `cron-${date}`;

    ctx.waitUntil(
      dispatchGithubWorkflow(env, requestId)
        .then((result) => {
          console.log(
            `[${requestId}] Scheduled workflow dispatch completed: ${JSON.stringify(result)}`
          );
        })
        .catch((error) => {
          console.error(`[${requestId}] Scheduled workflow dispatch failed:`, error);
          throw error;
        })
    );
  }
} satisfies ExportedHandler<Env>;

function normalizePath(pathname: string): string {
  return pathname.replace(/\/+$/, "") || "/";
}

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8"
    }
  });
}

function toErrorResponse(error: unknown): Response {
  if (error instanceof HttpError) {
    return jsonResponse({ ok: false, error: error.message }, error.status);
  }

  console.error("Unhandled error", error);
  const message = error instanceof Error ? error.message : "Unexpected error";
  return jsonResponse({ ok: false, error: message }, 500);
}

function assertMethod(request: Request, allowedMethods: string[]): void {
  const method = request.method.toUpperCase();

  if (!allowedMethods.includes(method)) {
    throw new HttpError(405, `Method ${method} is not allowed`);
  }
}

function assertAuthorized(request: Request, env: Env): void {
  const expectedToken = env.EXPORTER_SITE_TOKEN;
  if (!expectedToken) {
    return;
  }

  const authorizationHeader = request.headers.get("authorization");
  const bearerToken = authorizationHeader?.startsWith("Bearer ")
    ? authorizationHeader.slice("Bearer ".length).trim()
    : null;
  const fallbackToken = request.headers.get("x-api-token") ?? request.headers.get("x-api-key");
  const providedToken = bearerToken ?? fallbackToken;

  if (!providedToken || providedToken !== expectedToken) {
    throw new HttpError(401, "Unauthorized");
  }
}

function resolveImportDate(dateParam: string | null): string {
  if (!dateParam || dateParam.trim() === "") {
    return getYesterdayUtcDate(Date.now());
  }

  const date = dateParam.trim();
  if (!DATE_PATTERN.test(date)) {
    throw new HttpError(400, "Invalid date format. Use YYYY-MM-DD");
  }

  const parsed = new Date(`${date}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== date) {
    throw new HttpError(400, "Invalid date value");
  }

  return date;
}

function getYesterdayUtcDate(referenceTime: number): string {
  const date = new Date(referenceTime);
  date.setUTCDate(date.getUTCDate() - 1);
  return date.toISOString().slice(0, 10);
}

function parseBooleanFlag(value: string | null): boolean {
  if (!value) {
    return false;
  }

  return ["1", "true", "yes", "y", "on"].includes(value.toLowerCase());
}

function createRequestId(): string {
  try {
    return crypto.randomUUID().slice(0, 8);
  } catch {
    return Math.random().toString(16).slice(2, 10);
  }
}

function parseGithubRepository(repository: string): { owner: string; repo: string } {
  const trimmed = repository.trim();
  const segments = trimmed.split("/").filter((segment) => segment.length > 0);
  if (segments.length !== 2) {
    throw new Error("GITHUB_REPOSITORY must use the format owner/repo");
  }

  return {
    owner: segments[0],
    repo: segments[1]
  };
}

function resolveGithubWorkflowRef(ref: string | undefined): string {
  const trimmed = ref?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : DEFAULT_GITHUB_WORKFLOW_REF;
}

async function dispatchGithubWorkflow(
  env: Env,
  requestId: string
): Promise<{ repository: string; workflow: string; ref: string; status: number }> {
  const repository = requireEnvValue(env.GITHUB_REPOSITORY, "GITHUB_REPOSITORY");
  const workflowId = requireEnvValue(env.GITHUB_WORKFLOW_ID, "GITHUB_WORKFLOW_ID");
  const workflowToken = requireEnvValue(
    env.GITHUB_WORKFLOW_DISPATCH_TOKEN,
    "GITHUB_WORKFLOW_DISPATCH_TOKEN"
  );
  const ref = resolveGithubWorkflowRef(env.GITHUB_WORKFLOW_REF);
  const { owner, repo } = parseGithubRepository(repository);

  const response = await fetch(
    `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/actions/workflows/${encodeURIComponent(workflowId)}/dispatches`,
    {
      method: "POST",
      headers: {
        accept: "application/vnd.github+json",
        authorization: `Bearer ${workflowToken}`,
        "content-type": "application/json",
        "user-agent": "byrdocs-logs-exporter"
      },
      body: JSON.stringify({ ref })
    }
  );

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(
      `GitHub workflow dispatch failed (${response.status} ${response.statusText}): ${errorBody || "<empty response>"}`
    );
  }

  console.log(
    `[${requestId}] Dispatched GitHub workflow '${workflowId}' for ${repository}@${ref}`
  );

  return {
    repository,
    workflow: workflowId,
    ref,
    status: response.status
  };
}

function parsePositiveInt(value: string, fieldName: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new HttpError(400, `${fieldName} must be a positive integer`);
  }

  return parsed;
}

function stripHostBrackets(hostname: string): string {
  const trimmed = hostname.trim();
  if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
    return trimmed.slice(1, -1);
  }

  return trimmed;
}

function normalizeHostForSocket(hostname: string): string {
  const trimmed = stripHostBrackets(hostname);
  if (!trimmed) {
    return trimmed;
  }

  if (trimmed.includes(":") && !(trimmed.startsWith("[") && trimmed.endsWith("]"))) {
    return `[${trimmed}]`;
  }

  return trimmed;
}

function resolveImportTimeoutMs(timeoutParam: string | null, envTimeout: string | undefined): number {
  if (timeoutParam && timeoutParam.trim()) {
    return parsePositiveInt(timeoutParam.trim(), "timeoutMs");
  }

  if (envTimeout && envTimeout.trim()) {
    return parsePositiveInt(envTimeout.trim(), "IMPORT_TIMEOUT_MS");
  }

  return DEFAULT_IMPORT_TIMEOUT_MS;
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, operationName: string): Promise<T> {
  let timeoutHandle: number | undefined;

  return new Promise<T>((resolve, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(new HttpError(504, `${operationName} timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    promise
      .then((value) => {
        if (timeoutHandle !== undefined) {
          clearTimeout(timeoutHandle);
        }
        resolve(value);
      })
      .catch((error) => {
        if (timeoutHandle !== undefined) {
          clearTimeout(timeoutHandle);
        }
        reject(error);
      });
  });
}

function isLogRecord(value: unknown): value is LogRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isValidIdentifier(identifier: string): boolean {
  return identifier.length > 0 && !identifier.includes("\u0000");
}

function quoteIdentifier(identifier: string): string {
  if (!isValidIdentifier(identifier)) {
    throw new Error(`Invalid SQL identifier: ${identifier}`);
  }

  return `"${identifier.replaceAll('"', '""')}"`;
}

function inferColumnType(key: string, value: unknown): string {
  if (key === TIMESTAMP_COLUMN) {
    return "TIMESTAMPTZ";
  }

  if (typeof value === "string") {
    return "TEXT";
  }

  if (typeof value === "number") {
    return Number.isInteger(value) ? "BIGINT" : "DOUBLE PRECISION";
  }

  if (typeof value === "boolean") {
    return "BOOLEAN";
  }

  return "JSONB";
}

function normalizeColumnType(column: ColumnMeta): NormalizedPgType {
  const dataType = column.data_type.toLowerCase();

  if (dataType === "timestamp with time zone") {
    return "timestamptz";
  }

  if (dataType === "bigint") {
    return "bigint";
  }

  if (dataType === "double precision") {
    return "double precision";
  }

  if (dataType === "boolean") {
    return "boolean";
  }

  if (dataType === "jsonb" || column.udt_name === "jsonb") {
    return "jsonb";
  }

  return "text";
}

function columnCastSuffix(column: ColumnMeta): string {
  const type = normalizeColumnType(column);

  if (type === "timestamptz") {
    return "::timestamptz";
  }

  if (type === "bigint") {
    return "::bigint";
  }

  if (type === "double precision") {
    return "::double precision";
  }

  if (type === "boolean") {
    return "::boolean";
  }

  if (type === "jsonb") {
    return "::jsonb";
  }

  return "::text";
}

function parseTimestamp(value: unknown): string | null {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString();
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    const milliseconds = value > 1_000_000_000_000 ? value : value * 1000;
    const parsedFromNumber = new Date(milliseconds);
    return Number.isNaN(parsedFromNumber.getTime()) ? null : parsedFromNumber.toISOString();
  }

  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const normalized = PLAIN_TIMESTAMP_PATTERN.test(trimmed)
    ? `${trimmed.replace(" ", "T")}Z`
    : trimmed;
  const parsed = new Date(normalized);

  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function parseBigIntValue(value: unknown): number | string | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.trunc(value);
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    return /^-?\d+$/.test(trimmed) ? trimmed : null;
  }

  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  return null;
}

function parseDoubleValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  return null;
}

function parseBooleanValue(value: unknown): boolean | null {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) {
      return true;
    }

    if (["0", "false", "no", "n", "off"].includes(normalized)) {
      return false;
    }
  }

  return null;
}

function normalizeValueForColumn(value: unknown, column: ColumnMeta): QueryParameter {
  if (value === undefined || value === null) {
    return null;
  }

  const type = normalizeColumnType(column);

  if (type === "timestamptz") {
    return parseTimestamp(value);
  }

  if (type === "bigint") {
    return parseBigIntValue(value);
  }

  if (type === "double precision") {
    return parseDoubleValue(value);
  }

  if (type === "boolean") {
    return parseBooleanValue(value);
  }

  if (type === "jsonb") {
    return JSON.stringify(value);
  }

  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "object") {
    return JSON.stringify(value);
  }

  return String(value);
}

function applySslModeToConnectionString(connectionString: string, sslMode: string | undefined): string {
  const parsed = new URL(connectionString);
  const normalizedSslMode = sslMode?.trim();

  if (normalizedSslMode) {
    parsed.searchParams.set("sslmode", normalizedSslMode);
  }

  return parsed.toString();
}

function resolveConnectionString(env: Env): string {
  const configuredSslMode = env.LOGS_DB_SSLMODE;

  if (env.HYPERDRIVE?.connectionString) {
    return env.HYPERDRIVE.connectionString;
  }

  if (env.LOGS_DB_URL) {
    return applySslModeToConnectionString(env.LOGS_DB_URL, configuredSslMode);
  }

  const host = requireEnvValue(env.LOGS_DB_HOST, "LOGS_DB_HOST");
  const port = requireEnvValue(env.LOGS_DB_PORT, "LOGS_DB_PORT");
  const username = requireEnvValue(env.LOGS_DB_USERNAME, "LOGS_DB_USERNAME");
  const password = requireEnvValue(env.LOGS_DB_PASSWORD, "LOGS_DB_PASSWORD");
  const database = requireEnvValue(env.LOGS_DB_DATABASE, "LOGS_DB_DATABASE");

  const normalizedHost = host.includes(":") && !host.startsWith("[") ? `[${host}]` : host;
  const connectionUrl = new URL(`postgresql://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${normalizedHost}:${port}/${database}`);

  return applySslModeToConnectionString(connectionUrl.toString(), configuredSslMode);
}

function requireEnvValue(value: string | undefined, name: string): string {
  if (!value) {
    throw new HttpError(500, `Missing environment variable: ${name}`);
  }

  return value;
}

function buildPostgresOptions(connectionString: string): PostgresConnectionOptions {
  const options: Record<string, unknown> = {
    max: 1,
    connect_timeout: 10,
    idle_timeout: 20,
    prepare: false
  };

  const parsed = new URL(connectionString);
  const socketHost = normalizeHostForSocket(parsed.hostname);

  if (socketHost.includes(":")) {
    options.host = [socketHost];
    options.port = [parsed.port ? Number(parsed.port) : 5432];
  }

  return options as PostgresConnectionOptions;
}

function createSqlClient(env: Env): Sql {
  const connectionString = resolveConnectionString(env);
  return postgres(connectionString, buildPostgresOptions(connectionString));
}

async function closeSqlClient(sql: Sql, requestId: string, context: string): Promise<void> {
  try {
    await sql.end({ timeout: 1 });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`[${requestId}] Failed to close SQL client after ${context}: ${message}`);
  }
}

function describeDatabaseTarget(env: Env): { host: string; port: string; database: string } {
  const connectionString = resolveConnectionString(env);
  const parsed = new URL(connectionString);
  return {
    host: normalizeHostForSocket(parsed.hostname),
    port: parsed.port || "5432",
    database: parsed.pathname.replace(/^\//, "")
  };
}

function safeDescribeDatabaseTarget(env: Env): { host: string; port: string; database: string } {
  try {
    return describeDatabaseTarget(env);
  } catch {
    return {
      host: "unknown",
      port: "unknown",
      database: "unknown"
    };
  }
}

function getDatabaseSocketAddress(env: Env): { hostname: string; port: number } {
  const connectionString = resolveConnectionString(env);
  const parsed = new URL(connectionString);

  return {
    hostname: stripHostBrackets(parsed.hostname),
    port: parsed.port ? Number(parsed.port) : 5432
  };
}

async function runTcpProbe(
  mode: "object" | "string",
  address: { hostname: string; port: number }
): Promise<TcpProbeResult> {
  const startedAt = Date.now();
  const { connect } = await import("cloudflare:sockets");
  const socketAddress = `${normalizeHostForSocket(address.hostname)}:${address.port}`;

  try {
    const socket =
      mode === "object"
        ? connect({ hostname: address.hostname, port: address.port })
        : connect(socketAddress);

    await withTimeout(socket.opened, DIAG_TIMEOUT_MS, `TCP ${mode} probe`);
    await socket.close();

    return {
      mode,
      ok: true,
      elapsedMs: Date.now() - startedAt,
      address: socketAddress
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);

    return {
      mode,
      ok: false,
      elapsedMs: Date.now() - startedAt,
      address: socketAddress,
      error: message
    };
  }
}

async function probeRawTcpConnection(
  env: Env,
  requestId: string
): Promise<
  | {
      ok: true;
      target: { hostname: string; port: number };
      objectProbe: TcpProbeResult;
      stringProbe: TcpProbeResult;
    }
  | {
      ok: false;
      error: string;
    }
> {
  try {
    const target = getDatabaseSocketAddress(env);
    const objectProbe = await runTcpProbe("object", target);
    const stringProbe = await runTcpProbe("string", target);

    console.log(
      `[${requestId}] TCP probe completed (object=${objectProbe.ok}, string=${stringProbe.ok})`
    );

    return {
      ok: true,
      target,
      objectProbe,
      stringProbe
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[${requestId}] TCP probe failed: ${message}`);

    return {
      ok: false,
      error: message
    };
  }
}

async function probeDatabase(
  env: Env,
  requestId: string
): Promise<
  | { ok: true; elapsedMs: number; target: { host: string; port: string; database: string } }
  | {
      ok: false;
      elapsedMs: number;
      target: { host: string; port: string; database: string };
      error: string;
    }
> {
  const target = safeDescribeDatabaseTarget(env);
  const startedAt = Date.now();
  const sql = createSqlClient(env);

  try {
    await withTimeout(sql.unsafe("SELECT 1"), DIAG_TIMEOUT_MS, "Database probe");
    const elapsedMs = Date.now() - startedAt;
    console.log(`[${requestId}] Database probe succeeded in ${elapsedMs}ms`);

    return {
      ok: true,
      elapsedMs,
      target
    };
  } catch (error) {
    const elapsedMs = Date.now() - startedAt;
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[${requestId}] Database probe failed in ${elapsedMs}ms: ${message}`);

    return {
      ok: false,
      elapsedMs,
      target,
      error: message
    };
  } finally {
    await closeSqlClient(sql, requestId, "database probe");
  }
}

async function gunzipToText(buffer: ArrayBuffer): Promise<string> {
  const stream = new Blob([buffer]).stream().pipeThrough(new DecompressionStream("gzip"));
  return new Response(stream).text();
}

async function downloadAndParseLogs(bucket: R2Bucket, date: string): Promise<LogRecord[]> {
  const objectKey = `${date}.json.gz`;
  const object = await bucket.get(objectKey);

  if (!object) {
    throw new HttpError(404, `Log file not found: ${objectKey}`);
  }

  const compressed = await object.arrayBuffer();
  const jsonText = await gunzipToText(compressed);

  let payload: unknown;
  try {
    payload = JSON.parse(jsonText);
  } catch {
    throw new HttpError(400, `Failed to parse JSON from ${objectKey}`);
  }

  if (!Array.isArray(payload)) {
    throw new HttpError(400, `Expected a JSON array in ${objectKey}`);
  }

  const records: LogRecord[] = [];
  for (const [index, item] of payload.entries()) {
    if (!isLogRecord(item)) {
      throw new HttpError(400, `Record at index ${index} is not an object`);
    }

    records.push(item);
  }

  return records;
}

function groupRecordsByDataset(records: LogRecord[]): Map<string, LogRecord[]> {
  const grouped = new Map<string, LogRecord[]>();

  for (const record of records) {
    const rawDataset = record.dataset;
    const dataset = typeof rawDataset === "string" && rawDataset.trim() ? rawDataset.trim() : "unknown";
    const existing = grouped.get(dataset);

    if (existing) {
      existing.push(record);
    } else {
      grouped.set(dataset, [record]);
    }
  }

  return grouped;
}

async function ensureImportTableExists(sql: Sql): Promise<void> {
  await sql.unsafe(`
    CREATE TABLE IF NOT EXISTS ${quoteIdentifier(IMPORT_TABLE)} (
      id SERIAL PRIMARY KEY,
      import_date DATE NOT NULL,
      records_count INTEGER NOT NULL,
      import_timestamp TIMESTAMPTZ DEFAULT NOW(),
      dataset TEXT NOT NULL,
      UNIQUE(import_date, dataset)
    )
  `);
}

async function checkImportExists(sql: Sql, date: string, dataset: string): Promise<number | null> {
  const rows = await sql.unsafe<{ records_count: number }[]>(
    `
      SELECT records_count
      FROM ${quoteIdentifier(IMPORT_TABLE)}
      WHERE import_date = $1::date AND dataset = $2
    `,
    [date, dataset]
  );

  return rows.length > 0 ? Number(rows[0].records_count) : null;
}

async function recordImport(
  sql: Sql,
  date: string,
  dataset: string,
  recordsCount: number
): Promise<void> {
  await sql.unsafe(
    `
      INSERT INTO ${quoteIdentifier(IMPORT_TABLE)} (import_date, records_count, dataset)
      VALUES ($1::date, $2::integer, $3::text)
      ON CONFLICT (import_date, dataset)
      DO UPDATE SET
        records_count = EXCLUDED.records_count,
        import_timestamp = NOW()
    `,
    [date, recordsCount, dataset]
  );
}

async function getTableColumns(sql: Sql, tableName: string): Promise<Map<string, ColumnMeta>> {
  const rows = await sql.unsafe<ColumnMeta[]>(
    `
      SELECT column_name, data_type, udt_name
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = $1
    `,
    [tableName]
  );

  const columns = new Map<string, ColumnMeta>();
  for (const row of rows) {
    columns.set(row.column_name, row);
  }

  return columns;
}

async function tableExists(sql: Sql, tableName: string): Promise<boolean> {
  const rows = await sql.unsafe<{ exists: boolean }[]>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = $1
      ) AS exists
    `,
    [tableName]
  );

  return rows[0]?.exists ?? false;
}

function getValidRecordEntries(record: LogRecord): Array<[string, unknown]> {
  const seen = new Set<string>();
  const entries: Array<[string, unknown]> = [];

  for (const [field, value] of Object.entries(record)) {
    if (field === IMPORTED_AT_COLUMN || !isValidIdentifier(field) || seen.has(field)) {
      continue;
    }

    seen.add(field);
    entries.push([field, value]);
  }

  return entries;
}

async function ensureTableExists(sql: Sql, tableName: string, sampleRecord: LogRecord): Promise<void> {
  const exists = await tableExists(sql, tableName);
  if (exists) {
    return;
  }

  const columnDefinitions = getValidRecordEntries(sampleRecord).map(
    ([field, value]) => `${quoteIdentifier(field)} ${inferColumnType(field, value)}`
  );
  columnDefinitions.push(`${quoteIdentifier(IMPORTED_AT_COLUMN)} TIMESTAMPTZ DEFAULT NOW()`);

  await sql.unsafe(
    `CREATE TABLE ${quoteIdentifier(tableName)} (${columnDefinitions.join(", ")})`
  );
}

function findSampleValue(records: LogRecord[], field: string): unknown {
  for (const record of records) {
    const value = record[field];
    if (value !== undefined && value !== null) {
      return value;
    }
  }

  return null;
}

async function ensureColumnsExist(
  sql: Sql,
  tableName: string,
  records: LogRecord[]
): Promise<Map<string, ColumnMeta>> {
  const existingColumns = await getTableColumns(sql, tableName);
  const existingColumnNames = new Set(existingColumns.keys());
  const newFields = new Set<string>();

  for (const record of records) {
    for (const field of Object.keys(record)) {
      if (field === IMPORTED_AT_COLUMN || !isValidIdentifier(field)) {
        continue;
      }

      if (!existingColumnNames.has(field)) {
        newFields.add(field);
      }
    }
  }

  for (const field of newFields) {
    const sampleValue = findSampleValue(records, field);
    const columnType = inferColumnType(field, sampleValue);

    await sql.unsafe(
      `ALTER TABLE ${quoteIdentifier(tableName)} ADD COLUMN IF NOT EXISTS ${quoteIdentifier(field)} ${columnType}`
    );
  }

  if (newFields.size > 0) {
    return getTableColumns(sql, tableName);
  }

  return existingColumns;
}

async function insertRecords(sql: Sql, tableName: string, records: LogRecord[]): Promise<void> {
  if (records.length === 0) {
    return;
  }

  await ensureTableExists(sql, tableName, records[0]);
  const columns = await ensureColumnsExist(sql, tableName, records);
  const insertableColumnNames = [...columns.keys()]
    .filter((columnName) => columnName !== IMPORTED_AT_COLUMN)
    .sort();

  if (insertableColumnNames.length === 0) {
    return;
  }

  for (let offset = 0; offset < records.length; offset += BATCH_SIZE) {
    const batch = records.slice(offset, offset + BATCH_SIZE);
    const placeholders: string[] = [];
    const values: QueryParameter[] = [];

    for (const record of batch) {
      const rowPlaceholders: string[] = [];

      for (const columnName of insertableColumnNames) {
        const columnMeta = columns.get(columnName);
        if (!columnMeta) {
          throw new Error(`Missing metadata for column ${columnName}`);
        }

        values.push(normalizeValueForColumn(record[columnName], columnMeta));
        rowPlaceholders.push(`$${values.length}${columnCastSuffix(columnMeta)}`);
      }

      placeholders.push(`(${rowPlaceholders.join(", ")})`);
    }

    const insertSql = `
      INSERT INTO ${quoteIdentifier(tableName)} (${insertableColumnNames
        .map((columnName) => quoteIdentifier(columnName))
        .join(", ")})
      VALUES ${placeholders.join(", ")}
    `;

    await sql.unsafe(insertSql, values);
  }
}

async function deleteExistingRecords(sql: Sql, tableName: string, date: string): Promise<number> {
  const tableColumns = await getTableColumns(sql, tableName);
  if (!tableColumns.has(TIMESTAMP_COLUMN)) {
    throw new Error(
      `Cannot force reimport for dataset ${tableName}: missing ${TIMESTAMP_COLUMN} column`
    );
  }

  const startTime = `${date}T00:00:00.000Z`;
  const endTime = `${date}T23:59:59.999Z`;

  const result = await sql.unsafe(
    `
      DELETE FROM ${quoteIdentifier(tableName)}
      WHERE ${quoteIdentifier(TIMESTAMP_COLUMN)} >= $1::timestamptz
        AND ${quoteIdentifier(TIMESTAMP_COLUMN)} <= $2::timestamptz
    `,
    [startTime, endTime]
  );

  return result.count ?? 0;
}

async function importLogsForDate(
  env: Env,
  date: string,
  force: boolean,
  requestId = "import"
): Promise<ImportResult> {
  const importStartedAt = Date.now();
  const sql = createSqlClient(env);

  try {
    console.log(`[${requestId}] Ensuring import tracking table`);
    await ensureImportTableExists(sql);

    console.log(`[${requestId}] Downloading and parsing logs for ${date}`);
    const records = await downloadAndParseLogs(env.ANALYTICS_BUCKET, date);
    console.log(`[${requestId}] Parsed ${records.length} records from ${date}`);
    const recordsByDataset = groupRecordsByDataset(records);
    const datasets: DatasetImportResult[] = [];

    for (const [dataset, datasetRecords] of recordsByDataset.entries()) {
      const datasetStartedAt = Date.now();
      console.log(
        `[${requestId}] Processing dataset '${dataset}' with ${datasetRecords.length} records`
      );
      const existingCount = await checkImportExists(sql, date, dataset);
      let deletedRecords = 0;

      if (existingCount !== null) {
        if (!force) {
          throw new HttpError(
            409,
            `Import already exists for dataset ${dataset} on ${date} (${existingCount} records). Use force=true to reimport.`
          );
        }

        deletedRecords = await deleteExistingRecords(sql, dataset, date);
      }

      await insertRecords(sql, dataset, datasetRecords);
      await recordImport(sql, date, dataset, datasetRecords.length);

      datasets.push({
        dataset,
        insertedRecords: datasetRecords.length,
        deletedRecords
      });

      console.log(
        `[${requestId}] Dataset '${dataset}' finished (deleted=${deletedRecords}, inserted=${datasetRecords.length}, elapsedMs=${Date.now() - datasetStartedAt})`
      );
    }

    console.log(
      `[${requestId}] Import completed for ${date} (records=${records.length}, datasets=${datasets.length}, elapsedMs=${Date.now() - importStartedAt})`
    );

    return {
      date,
      totalRecords: records.length,
      datasets
    };
  } finally {
    await closeSqlClient(sql, requestId, `import ${date}`);
  }
}

async function listBucketObjects(bucket: R2Bucket, prefix?: string): Promise<{
  objects: Array<{ key: string; size: number; uploaded: string }>;
  truncated: boolean;
  prefix: string;
}> {
  const objects: Array<{ key: string; size: number; uploaded: string }> = [];
  let cursor: string | undefined;
  let truncated = false;
  const normalizedPrefix = prefix ?? "";

  for (let page = 0; page < MAX_LIST_PAGES; page += 1) {
    const listOptions: R2ListOptions = {
      limit: 1000,
      prefix: normalizedPrefix
    };

    if (cursor) {
      listOptions.cursor = cursor;
    }

    const listResult = await bucket.list(listOptions);
    for (const object of listResult.objects) {
      objects.push({
        key: object.key,
        size: object.size,
        uploaded: object.uploaded.toISOString()
      });
    }

    if (!listResult.truncated || !listResult.cursor) {
      truncated = false;
      break;
    }

    truncated = true;
    cursor = listResult.cursor;
  }

  return {
    objects,
    truncated,
    prefix: normalizedPrefix
  };
}
