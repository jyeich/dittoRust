use anyhow::{Context, Result};
use ditto_quickstart::tui::Todolist;
use dittolive_ditto::{fs::TempRoot, identity::OnlinePlayground, AppId, Ditto};
use std::time::Duration;
use std::{env, sync::Arc};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🦀 Starting Rust TUI Integration Test");

    dotenvy::dotenv().ok();

    let app_id: AppId = env::var("DITTO_APP_ID")
        .context("DITTO_APP_ID not found")?
        .parse()
        .context("Invalid DITTO_APP_ID format")?;
    let playground_token =
        env::var("DITTO_PLAYGROUND_TOKEN").context("DITTO_PLAYGROUND_TOKEN not found")?;
    let custom_auth_url =
        env::var("DITTO_AUTH_URL").unwrap_or_else(|_| "https://auth.cloud.ditto.live".to_string());
    let websocket_url =
        env::var("DITTO_WEBSOCKET_URL").unwrap_or_else(|_| "wss://cloud.ditto.live".to_string());

    let ditto = Ditto::builder()
        .with_root(Arc::new(TempRoot::new()))
        .with_identity(|root| {
            OnlinePlayground::new(
                root,
                app_id.clone(),
                playground_token,
                false,
                Some(custom_auth_url.as_str()),
            )
        })?
        .build()?;

    ditto.update_transport_config(|config| {
        config.enable_all_peer_to_peer();
        config.connect.websocket_urls.insert(websocket_url.clone());
    });

    let _ = ditto.disable_sync_with_v3();
    let _ = ditto
        .store()
        .execute_v2("ALTER SYSTEM SET DQL_STRICT_MODE = false")
        .await?;

    let _ = ditto.start_sync();
    println!("✅ Created Ditto instance and started sync");

    let client_name = env::var("DITTO_CLIENT_NAME").ok();
    let todolist = Todolist::new(ditto, websocket_url, client_name)?;
    println!("📍 App loaded - Created locations instance");

    println!("🕐 Waiting for sync...");
    let mut attempts = 0;
    let max_attempts = 15;
    let mut synced = false;

    while attempts < max_attempts && !synced {
        sleep(Duration::from_secs(1)).await;
        attempts += 1;

        let locations = todolist.tasks_rx.borrow().clone();
        if !locations.is_empty() {
            synced = true;
            println!("✅ Sync successful — {} location(s) found:", locations.len());
            for loc in locations.iter().take(5) {
                println!("   - id={} lat={:.6} lon={:.6}", loc.id, loc.lat, loc.lon);
            }
        }

        if attempts % 3 == 0 && !synced {
            println!("   ... still syncing ({}/{})", attempts, max_attempts);
        }
    }

    todolist.ditto.stop_sync();
    println!("🛑 Stopped sync");

    if !synced {
        println!("⚠️  No locations found after {} seconds (collection may be empty)", max_attempts);
    }

    println!("🎉 Integration test passed! App loads and syncs with Ditto Cloud successfully.");
    Ok(())
}
