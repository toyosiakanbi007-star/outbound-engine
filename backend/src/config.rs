use std::{env, str::FromStr};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, Copy)]
pub enum AppEnv {
    Development,
    Staging,
    Production,
}

impl FromStr for AppEnv {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "development" | "dev" => Ok(AppEnv::Development),
            "staging" | "stage" => Ok(AppEnv::Staging),
            "production" | "prod" => Ok(AppEnv::Production),
            _ => Ok(AppEnv::Development), // default if unknown
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub env: AppEnv,
    pub database_url: String,
    pub http_port: u16,

    pub ses_region: Option<String>,
    pub ses_access_key: Option<String>,
    pub ses_secret_key: Option<String>,

    pub apollo_api_key: Option<String>,
    pub prospeo_api_key: Option<String>,

    // Optional HTTP news service (for local/mock /news/fetch microservice).
    // In production we now prefer direct Lambda invocation via NEWS_LAMBDA_FUNCTION_NAME,
    // which is read directly in news::client::build_news_client (not stored here).
    pub news_service_base_url: Option<String>,
    pub news_service_api_key: Option<String>,

    // LLM provider selection (for future Bedrock/OpenAI/etc.)
    // e.g. "dummy", "bedrock", "openai"
    pub llm_provider: Option<String>,
}

/// Entry point to load configuration
pub fn load() -> Result<Config> {
    load_dotenv()?;
    Config::from_env()
}

/// Load .env base, then .env.{APP_ENV}
fn load_dotenv() -> Result<()> {
    // 1. Load base .env (if it exists)
    let _ = dotenvy::dotenv();

    // 2. Read APP_ENV from env (may come from .env)
    let env_name = env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());

    // 3. Try to load .env.{APP_ENV}, e.g. .env.development
    let filename = format!(".env.{}", env_name);
    let _ = dotenvy::from_filename(&filename);

    Ok(())
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let env_str = env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());
        let env = AppEnv::from_str(&env_str).unwrap_or(AppEnv::Development);

        let database_url = env::var("DATABASE_URL")
            .map_err(|_| "DATABASE_URL env var is required")?;

        let http_port: u16 = env::var("HTTP_PORT")
            .unwrap_or_else(|_| "3000".to_string())
            .parse()
            .map_err(|_| "HTTP_PORT must be a valid u16")?;

        let ses_region = env::var("SES_REGION").ok();
        let ses_access_key = env::var("SES_ACCESS_KEY_ID").ok();
        let ses_secret_key = env::var("SES_SECRET_ACCESS_KEY").ok();

        let apollo_api_key = env::var("APOLLO_API_KEY").ok();
        let prospeo_api_key = env::var("PROSPEO_API_KEY").ok();

        let news_service_base_url = env::var("NEWS_SERVICE_BASE_URL").ok();
        let news_service_api_key = env::var("NEWS_SERVICE_API_KEY").ok();

        // Optional: which LLM provider the system should use (future hook)
        let llm_provider = env::var("LLM_PROVIDER").ok();

        Ok(Self {
            env,
            database_url,
            http_port,
            ses_region,
            ses_access_key,
            ses_secret_key,
            apollo_api_key,
            prospeo_api_key,
            news_service_base_url,
            news_service_api_key,
            llm_provider,
        })
    }
}
