use anyhow::{Context, Result};
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client as S3Client, Config as S3Config};
use chrono::{NaiveDate, Utc};
use clap::{Parser, Subcommand};
use flate2::read::GzDecoder;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use std::io::Read;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Import logs for a specific date
    Import {
        /// Date in YYYY-MM-DD format
        date: String,
        /// Force reimport even if already imported
        #[arg(long)]
        force: bool,
    },
    /// Watch and automatically import logs daily at 1:00 UTC
    Watch,
    /// List available files in the R2 bucket
    List,
}

#[derive(Debug, Clone)]
struct Config {
    r2_access_id: String,
    r2_access_secret: String,
    r2_endpoint: String,
    r2_bucket: String,
    db_host: String,
    db_port: u16,
    db_username: String,
    db_password: String,
    db_database: String,
}

impl Config {
    fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();
        
        Ok(Config {
            r2_access_id: std::env::var("R2_ACCESS_ID")
                .context("R2_ACCESS_ID environment variable not found")?,
            r2_access_secret: std::env::var("R2_ACCESS_SECRET")
                .context("R2_ACCESS_SECRET environment variable not found")?,
            r2_endpoint: std::env::var("R2_ENDPOINT")
                .context("R2_ENDPOINT environment variable not found")?,
            r2_bucket: std::env::var("R2_BUCKET")
                .context("R2_BUCKET environment variable not found")?,
            db_host: std::env::var("DB_HOST")
                .context("DB_HOST environment variable not found")?,
            db_port: std::env::var("DB_PORT")
                .context("DB_PORT environment variable not found")?
                .parse()
                .context("Invalid DB_PORT")?,
            db_username: std::env::var("DB_USERNAME")
                .context("DB_USERNAME environment variable not found")?,
            db_password: std::env::var("DB_PASSWORD")
                .context("DB_PASSWORD environment variable not found")?,
            db_database: std::env::var("DB_DATABASE")
                .context("DB_DATABASE environment variable not found")?,
        })
    }
}

async fn create_s3_client(config: &Config) -> Result<S3Client> {
    info!("Creating S3 client");
    info!("  Endpoint: {}", config.r2_endpoint);
    info!("  Access ID: {}...", &config.r2_access_id[..config.r2_access_id.len().min(8)]);
    
    let credentials = Credentials::new(
        &config.r2_access_id,
        &config.r2_access_secret,
        None,
        None,
        "static",
    );

    let s3_config = S3Config::builder()
        .credentials_provider(credentials)
        .endpoint_url(&config.r2_endpoint)
        .region(aws_sdk_s3::config::Region::new("auto"))
        .force_path_style(true)
        .build();

    let client = S3Client::from_conf(s3_config);
    info!("S3 client created successfully");
    Ok(client)
}

async fn create_db_pool(config: &Config) -> Result<PgPool> {
    let database_url = format!(
        "postgresql://{}:{}@{}:{}/{}",
        config.db_username, config.db_password, config.db_host, config.db_port, config.db_database
    );

    PgPool::connect(&database_url)
        .await
        .context("Failed to connect to PostgreSQL")
}

async fn download_and_decompress_logs(s3_client: &S3Client, bucket: &str, date: &str) -> Result<Vec<u8>> {
    let key = format!("{}.json.gz", date);
    
    info!("Attempting to download log file");
    info!("  Bucket: {}", bucket);
    info!("  Key: {}", key);
    info!("  Full path: {}/{}", bucket, key);
    
    let response = s3_client
        .get_object()
        .bucket(bucket)
        .key(&key)
        .send()
        .await
        .context(format!("Failed to download file from R2. Bucket: {}, Key: {}", bucket, key))?;

    let compressed_data = response
        .body
        .collect()
        .await
        .context("Failed to read response body")?
        .into_bytes();

    info!("Downloaded {} bytes, decompressing...", compressed_data.len());

    let mut decoder = GzDecoder::new(compressed_data.as_ref());
    let mut decompressed_data = Vec::new();
    decoder
        .read_to_end(&mut decompressed_data)
        .context("Failed to decompress gzip data")?;

    info!("Decompressed to {} bytes", decompressed_data.len());
    Ok(decompressed_data)
}

async fn ensure_table_exists(pool: &PgPool, table_name: &str, sample_record: &Value) -> Result<()> {
    let table_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)"
    )
    .bind(table_name)
    .fetch_one(pool)
    .await
    .context("Failed to check if table exists")?;

    if !table_exists {
        info!("Creating table: {}", table_name);
        
        let mut columns = Vec::new();
        
        if let Value::Object(obj) = sample_record {
            for (key, value) in obj {
                let column_type = if key == "timestamp" {
                    "TIMESTAMP WITH TIME ZONE"
                } else {
                    match value {
                        Value::String(_) => "TEXT",
                        Value::Number(n) => {
                            if n.is_f64() {
                                "DOUBLE PRECISION"
                            } else {
                                "BIGINT"
                            }
                        }
                        Value::Bool(_) => "BOOLEAN",
                        _ => "JSONB",
                    }
                };
                columns.push(format!("{} {}", key, column_type));
            }
        }

        columns.push("imported_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()".to_string());

        let create_table_sql = format!(
            "CREATE TABLE {} ({})",
            table_name,
            columns.join(", ")
        );

        sqlx::query(&create_table_sql)
            .execute(pool)
            .await
            .context("Failed to create table")?;

        info!("Table {} created successfully", table_name);
    }

    Ok(())
}

async fn insert_records(pool: &PgPool, table_name: &str, records: &[Value]) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let sample_record = &records[0];
    ensure_table_exists(pool, table_name, sample_record).await?;

    // Get column names from the first record
    let columns: Vec<String> = if let Value::Object(obj) = sample_record {
        obj.keys().cloned().collect()
    } else {
        return Ok(());
    };

    let batch_size = 100; // Insert in batches of 100 records
    let mut tx = pool.begin().await.context("Failed to begin transaction")?;

    for batch in records.chunks(batch_size) {
        let mut values_clauses = Vec::new();
        let mut bind_values = Vec::new();
        let mut param_index = 1;

        for record in batch {
            if let Value::Object(obj) = record {
                let mut record_placeholders = Vec::new();
                
                for column in &columns {
                    record_placeholders.push(format!("${}", param_index));
                    param_index += 1;
                    
                    let value = obj.get(column).unwrap_or(&Value::Null);
                    bind_values.push(value);
                }
                
                values_clauses.push(format!("({})", record_placeholders.join(", ")));
            }
        }

        if !values_clauses.is_empty() {
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                table_name,
                columns.join(", "),
                values_clauses.join(", ")
            );

            let mut query = sqlx::query(&insert_sql);
            
            for (i, value) in bind_values.iter().enumerate() {
                let column_name = &columns[i % columns.len()];
                
                if column_name == "timestamp" && matches!(value, Value::String(_)) {
                    // Parse timestamp and convert to UTC timezone
                    if let Value::String(timestamp_str) = value {
                        let timestamp_with_tz = format!("{} UTC", timestamp_str);
                        query = query.bind(timestamp_with_tz);
                    } else {
                        query = query.bind(Option::<String>::None);
                    }
                } else {
                    match value {
                        Value::String(s) => {
                            query = query.bind(s);
                        }
                        Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                query = query.bind(i);
                            } else if let Some(f) = n.as_f64() {
                                query = query.bind(f);
                            } else {
                                query = query.bind(n.to_string());
                            }
                        }
                        Value::Bool(b) => {
                            query = query.bind(b);
                        }
                        Value::Null => {
                            query = query.bind(Option::<String>::None);
                        }
                        _ => {
                            query = query.bind(value.to_string());
                        }
                    }
                }
            }

            query.execute(&mut *tx).await.context("Failed to insert batch")?;
            info!("Inserted batch of {} records", batch.len());
        }
    }

    tx.commit().await.context("Failed to commit transaction")?;
    info!("Successfully inserted {} records into table {}", records.len(), table_name);

    Ok(())
}

async fn ensure_import_table_exists(pool: &PgPool) -> Result<()> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS _import (
            id SERIAL PRIMARY KEY,
            import_date DATE NOT NULL,
            records_count INTEGER NOT NULL,
            import_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            dataset TEXT NOT NULL,
            UNIQUE(import_date, dataset)
        )
    "#)
    .execute(pool)
    .await
    .context("Failed to create _import table")?;
    
    Ok(())
}

async fn check_import_exists(pool: &PgPool, date: &str, dataset: &str) -> Result<Option<i32>> {
    let result = sqlx::query_scalar::<_, Option<i32>>(
        "SELECT records_count FROM _import WHERE import_date = $1::date AND dataset = $2"
    )
    .bind(date)
    .bind(dataset)
    .fetch_optional(pool)
    .await
    .context("Failed to check import status")?;
    
    Ok(result.flatten())
}

async fn record_import(pool: &PgPool, date: &str, dataset: &str, records_count: i32) -> Result<()> {
    sqlx::query(r#"
        INSERT INTO _import (import_date, records_count, dataset)
        VALUES ($1::date, $2, $3)
        ON CONFLICT (import_date, dataset) 
        DO UPDATE SET 
            records_count = EXCLUDED.records_count,
            import_timestamp = NOW()
    "#)
    .bind(date)
    .bind(records_count)
    .bind(dataset)
    .execute(pool)
    .await
    .context("Failed to record import")?;
    
    Ok(())
}

async fn delete_existing_records(pool: &PgPool, table_name: &str, date: &str) -> Result<i64> {
    let start_time = format!("{} 00:00:00", date);
    let end_time = format!("{} 23:59:59", date);
    
    let result = sqlx::query(
        &format!("DELETE FROM {} WHERE timestamp >= $1 AND timestamp <= $2", table_name)
    )
    .bind(&start_time)
    .bind(&end_time)
    .execute(pool)
    .await
    .context("Failed to delete existing records")?;
    
    Ok(result.rows_affected() as i64)
}

async fn import_logs_for_date(config: &Config, date: &str, force: bool) -> Result<()> {
    info!("Starting import process for date: {}", date);
    
    let s3_client = create_s3_client(config).await?;
    let pool = create_db_pool(config).await?;
    
    ensure_import_table_exists(&pool).await?;

    let decompressed_data = download_and_decompress_logs(&s3_client, &config.r2_bucket, date).await?;
    
    let json_str = String::from_utf8(decompressed_data)
        .context("Failed to convert decompressed data to UTF-8")?;

    let records: Vec<Value> = serde_json::from_str(&json_str)
        .context("Failed to parse JSON data")?;

    info!("Parsed {} records", records.len());

    let mut records_by_dataset: HashMap<String, Vec<Value>> = HashMap::new();

    for record in records {
        if let Value::Object(ref obj) = record {
            let dataset = obj
                .get("dataset")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();

            records_by_dataset
                .entry(dataset)
                .or_insert_with(Vec::new)
                .push(record);
        }
    }

    for (dataset, records) in records_by_dataset {
        info!("Processing {} records for dataset: {}", records.len(), dataset);
        
        // Check if already imported
        if let Some(existing_count) = check_import_exists(&pool, date, &dataset).await? {
            if !force {
                error!("FATAL: Dataset '{}' for date {} already imported with {} records. Use --force to reimport.", 
                       dataset, date, existing_count);
                return Err(anyhow::anyhow!(
                    "Import already exists for dataset '{}' on date {}. Use --force to reimport.", 
                    dataset, date
                ));
            } else {
                info!("Force reimport requested. Deleting existing records for dataset '{}' on date {}", dataset, date);
                let deleted_count = delete_existing_records(&pool, &dataset, date).await?;
                info!("Deleted {} existing records", deleted_count);
            }
        }
        
        insert_records(&pool, &dataset, &records).await?;
        record_import(&pool, date, &dataset, records.len() as i32).await?;
        info!("Successfully imported {} records for dataset: {}", records.len(), dataset);
    }

    info!("Successfully imported logs for date: {}", date);
    Ok(())
}

async fn run_import(date: String, force: bool) -> Result<()> {
    let config = Config::from_env()?;
    
    NaiveDate::parse_from_str(&date, "%Y-%m-%d")
        .context("Invalid date format. Use YYYY-MM-DD")?;

    import_logs_for_date(&config, &date, force).await
}

async fn run_watch() -> Result<()> {
    let config = Config::from_env()?;
    
    info!("Starting watch mode - will import logs daily at 1:00 UTC");

    let mut scheduler = JobScheduler::new().await?;

    let job_config = config.clone();
    let job = Job::new_async("0 0 1 * * *", move |_uuid, _l| {
        let config = job_config.clone();
        Box::pin(async move {
            let yesterday = (Utc::now() - chrono::Duration::days(1))
                .format("%Y-%m-%d")
                .to_string();

            info!("Running scheduled import for date: {}", yesterday);

            match import_logs_for_date(&config, &yesterday, false).await {
                Ok(()) => info!("Scheduled import completed successfully for {}", yesterday),
                Err(e) => error!("Scheduled import failed for {}: {}", yesterday, e),
            }
        })
    })?;

    scheduler.add(job).await?;
    scheduler.start().await?;

    info!("Scheduler started. Press Ctrl+C to stop.");

    tokio::signal::ctrl_c().await?;
    info!("Shutting down scheduler...");
    scheduler.shutdown().await?;

    Ok(())
}

async fn run_list() -> Result<()> {
    let config = Config::from_env()?;
    let s3_client = create_s3_client(&config).await?;
    
    info!("Listing files in bucket: {}", config.r2_bucket);
    
    let response = s3_client
        .list_objects_v2()
        .bucket(&config.r2_bucket)
        .send()
        .await
        .context("Failed to list objects in R2 bucket")?;
    
    let contents = response.contents();
    if !contents.is_empty() {
        info!("Found {} objects in bucket:", contents.len());
        for object in contents {
            if let Some(key) = object.key() {
                let size = object.size().unwrap_or(0);
                let last_modified = object.last_modified()
                    .map(|dt| dt.to_string())
                    .unwrap_or_else(|| "Unknown".to_string());
                info!("  {} ({} bytes, modified: {})", key, size, last_modified);
            }
        }
    } else {
        info!("No objects found in bucket");
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set default log level to info if RUST_LOG is not set
    let _guard = if std::env::var("RUST_LOG").is_err() {
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .init();
    } else {
        tracing_subscriber::fmt::init();
    };

    let cli = Cli::parse();

    match cli.command {
        Commands::Import { date, force } => run_import(date, force).await,
        Commands::Watch => run_watch().await,
        Commands::List => run_list().await,
    }
}
