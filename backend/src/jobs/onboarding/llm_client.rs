// src/jobs/onboarding/llm_client.rs
//
// Direct Azure OpenAI client for onboarding LLM calls.
// No Azure Function middleman — calls the REST API directly.
//
// Required env vars:
//   AZURE_OPENAI_ENDPOINT    — e.g. https://my-resource.openai.azure.com
//   AZURE_OPENAI_API_KEY     — API key
//   AZURE_OPENAI_DEPLOYMENT  — deployment name (e.g. gpt-5-mini)
//   AZURE_OPENAI_API_VERSION — e.g. 2024-12-01-preview

use reqwest::Client as HttpClient;
use serde_json::{json, Value as JsonValue};
use tracing::{debug, error, info};

lazy_static::lazy_static! {
    static ref ENDPOINT: String = std::env::var("AZURE_OPENAI_ENDPOINT")
        .unwrap_or_default();
    static ref API_KEY: String = std::env::var("AZURE_OPENAI_API_KEY")
        .unwrap_or_default();
    static ref DEPLOYMENT: String = std::env::var("AZURE_OPENAI_DEPLOYMENT")
        .unwrap_or_else(|_| "gpt-4o".to_string());
    static ref API_VERSION: String = std::env::var("AZURE_OPENAI_API_VERSION")
        .unwrap_or_else(|_| "2024-12-01-preview".to_string());
}

/// The model string used (for logging/storage)
pub fn model_name() -> String {
    DEPLOYMENT.clone()
}

/// Call Azure OpenAI chat completion.
///
/// Does NOT send temperature (some models like gpt-5-mini only support default=1).
pub async fn chat_completion(
    http_client: &HttpClient,
    system_prompt: &str,
    user_prompt: &str,
    max_tokens: u32,
) -> Result<String, String> {
    if ENDPOINT.is_empty() || API_KEY.is_empty() {
        return Err("AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_API_KEY not configured".into());
    }

    let url = format!(
        "{}/openai/deployments/{}/chat/completions?api-version={}",
        ENDPOINT.trim_end_matches('/'),
        *DEPLOYMENT,
        *API_VERSION,
    );

    let body = json!({
        "messages": [
            { "role": "system", "content": system_prompt },
            { "role": "user", "content": user_prompt },
        ],
        "max_completion_tokens": max_tokens,
        "response_format": { "type": "json_object" },
    });

    debug!("Azure OpenAI request to deployment={}, max_completion_tokens={}", *DEPLOYMENT, max_tokens);

    let resp = http_client
        .post(&url)
        .header("api-key", API_KEY.as_str())
        .header("Content-Type", "application/json")
        .timeout(std::time::Duration::from_secs(300))
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Azure OpenAI request failed: {}", e))?;

    let status = resp.status();
    let resp_text = resp.text().await.map_err(|e| format!("Failed to read response: {}", e))?;

    if !status.is_success() {
        error!("Azure OpenAI error {}: {}", status, &resp_text[..resp_text.len().min(500)]);
        return Err(format!("Azure OpenAI returned {}: {}", status, &resp_text[..resp_text.len().min(300)]));
    }

    let resp_json: JsonValue = serde_json::from_str(&resp_text)
        .map_err(|e| format!("Failed to parse Azure OpenAI response: {}", e))?;

    let content = resp_json["choices"][0]["message"]["content"]
        .as_str()
        .ok_or_else(|| "No content in Azure OpenAI response".to_string())?;

    info!("Azure OpenAI response: {} chars from {}", content.len(), *DEPLOYMENT);

    Ok(content.to_string())
}

/// Call chat completion and parse the response as JSON.
pub async fn chat_completion_json(
    http_client: &HttpClient,
    system_prompt: &str,
    user_prompt: &str,
    max_tokens: u32,
) -> Result<JsonValue, String> {
    let text = chat_completion(http_client, system_prompt, user_prompt, max_tokens).await?;

    // Strip markdown fences if present
    let clean = text
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    serde_json::from_str(clean)
        .map_err(|e| format!("Failed to parse LLM JSON output: {}. Raw: {}...", e, &clean[..clean.len().min(200)]))
}
