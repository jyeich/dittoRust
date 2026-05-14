use anyhow::{Context, Result};
use chrono::Utc;
use dittolive_ditto::Ditto;
use quick_xml::events::Event as XmlEvent;
use quick_xml::Reader;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::UdpSocket;
use tokio::sync::watch;

use crate::tui::todolist::LocationItem;

/// Tracks UIDs and the exact position inserted by this app's UDP listener so
/// the sender can skip re-broadcasting that specific position back out.
/// Keyed by uid, value is the (lat, lon) that was locally inserted.
pub type LocalUids = Arc<Mutex<HashMap<String, (f64, f64)>>>;

pub fn new_local_uids() -> LocalUids {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Bind a UDP socket on `port`, parse incoming CoT XML, and upsert each entity
/// into the Ditto `locations` collection. Inserted UIDs are recorded in `local_uids`
/// so the sender knows not to echo them back out.
pub async fn run_udp_listener(
    ditto: Arc<Ditto>,
    port: u16,
    local_uids: LocalUids,
) -> Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let socket = UdpSocket::bind(&addr)
        .await
        .with_context(|| format!("failed to bind UDP socket on {}", addr))?;
    tracing::info!("UDP CoT listener bound on {}", addr);

    let mut buf = vec![0u8; 65535];

    loop {
        let (len, peer) = socket.recv_from(&mut buf).await?;
        let raw = match std::str::from_utf8(&buf[..len]) {
            Ok(s) => s,
            Err(_) => {
                tracing::warn!(from = %peer, "received non-UTF8 UDP packet, skipping");
                continue;
            }
        };

        match parse_cot(raw) {
            Some((uid, lat, lon)) => {
                tracing::info!(from = %peer, %uid, %lat, %lon, "received CoT");
                // Record the exact position we're inserting so the sender
                // can suppress only this specific (uid, lat, lon), not all
                // future updates to this uid from remote peers.
                if let Ok(mut map) = local_uids.lock() {
                    map.insert(uid.clone(), (lat, lon));
                }
                if let Err(e) = upsert_location(&ditto, uid, lat, lon).await {
                    tracing::error!(%e, "failed to upsert CoT location");
                }
            }
            None => {
                tracing::warn!(from = %peer, "failed to parse CoT XML, skipping");
            }
        }
    }
}

/// Watch the Ditto `locations` collection for changes that originated from remote
/// peers (i.e. not in `local_uids`) and forward them as CoT XML to `output_addr`.
pub async fn run_udp_sender(
    ditto: Arc<Ditto>,
    output_addr: String,
    local_uids: LocalUids,
) -> Result<()> {
    // Bind on an ephemeral port for sending
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket
        .connect(&output_addr)
        .await
        .with_context(|| format!("failed to connect UDP sender to {}", output_addr))?;
    tracing::info!("UDP CoT sender connected to {}", output_addr);

    let (tx, mut rx) = watch::channel(Vec::<LocationItem>::new());

    // Register a Ditto observer — fires whenever the collection changes
    let _observer = ditto.store().register_observer_v2(
        "SELECT * FROM locations WHERE deleted=false ORDER BY _id ASC",
        move |query_result| {
            let docs = query_result
                .into_iter()
                .flat_map(|it| it.deserialize_value::<LocationItem>().ok())
                .collect::<Vec<_>>();
            tx.send_replace(docs);
        },
    )?;

    // prev tracks the last known (lat, lon) per uid so we only send on change
    let mut prev: HashMap<String, (f64, f64)> = HashMap::new();

    loop {
        rx.changed().await?;
        let locations = rx.borrow().clone();

        // Collect XML strings to send while holding the lock briefly, then send outside
        let to_send: Vec<String> = {
            let local = local_uids
                .lock()
                .map_err(|_| anyhow::anyhow!("local_uids lock poisoned"))?;

            locations
                .iter()
                .filter(|loc| match local.get(&loc.uid) {
                        None => true,
                        // Only suppress if the position exactly matches what we inserted.
                        // A remote update with different coords must still be forwarded.
                        Some(&(plat, plon)) => {
                            (plat - loc.lat).abs() > 1e-10 || (plon - loc.lon).abs() > 1e-10
                        }
                    })
                .filter(|loc| match prev.get(&loc.uid) {
                    None => true, // new remote item
                    Some(&(plat, plon)) => {
                        (plat - loc.lat).abs() > 1e-10 || (plon - loc.lon).abs() > 1e-10
                    }
                })
                .map(|loc| format_cot(&loc.uid, loc.lat, loc.lon))
                .collect()
            // lock released here
        };

        for xml in &to_send {
            if let Err(e) = socket.send(xml.as_bytes()).await {
                tracing::error!(%e, "failed to send CoT UDP packet");
            }
        }

        if !to_send.is_empty() {
            tracing::debug!("forwarded {} location(s) to {}", to_send.len(), output_addr);
        }

        // Update previous state snapshot
        prev = locations
            .iter()
            .map(|l| (l.uid.clone(), (l.lat, l.lon)))
            .collect();
    }
}

/// Format a Ditto location as a CoT XML string.
fn format_cot(uid: &str, lat: f64, lon: f64) -> String {
    let now = Utc::now();
    let time_str = now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let stale_str = (now + chrono::Duration::seconds(75))
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><event version="2.0" uid="{uid}" type="a-f-G-U-C" time="{time}" start="{time}" stale="{stale}" how="m-g"><point lat="{lat:.10}" lon="{lon:.10}" hae="9999999.0" ce="9999999.0" le="9999999.0"/><detail></detail></event>"#,
        uid = uid,
        lat = lat,
        lon = lon,
        time = time_str,
        stale = stale_str,
    )
}

/// Parse a CoT XML message and return `(uid, lat, lon)`.
fn parse_cot(xml: &str) -> Option<(String, f64, f64)> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut uid: Option<String> = None;
    let mut lat: Option<f64> = None;
    let mut lon: Option<f64> = None;

    loop {
        match reader.read_event() {
            Ok(XmlEvent::Start(ref e)) | Ok(XmlEvent::Empty(ref e)) => {
                match e.name().as_ref() {
                    b"event" => {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"uid" {
                                uid = std::str::from_utf8(&attr.value).ok().map(str::to_owned);
                            }
                        }
                    }
                    b"point" => {
                        for attr in e.attributes().flatten() {
                            match attr.key.as_ref() {
                                b"lat" => {
                                    lat = std::str::from_utf8(&attr.value)
                                        .ok()
                                        .and_then(|s| s.parse().ok());
                                }
                                b"lon" => {
                                    lon = std::str::from_utf8(&attr.value)
                                        .ok()
                                        .and_then(|s| s.parse().ok());
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(XmlEvent::Eof) | Err(_) => break,
            _ => {}
        }
    }

    Some((uid?, lat?, lon?))
}

/// Insert a new location or update an existing one matched by `uid`.
async fn upsert_location(ditto: &Arc<Ditto>, uid: String, lat: f64, lon: f64) -> Result<()> {
    let result = ditto
        .store()
        .execute_v2((
            "SELECT * FROM locations WHERE uid=:uid AND deleted=false",
            serde_json::json!({ "uid": uid }),
        ))
        .await?;

    let existing = result
        .into_iter()
        .next()
        .and_then(|item| item.deserialize_value::<LocationItem>().ok());

    match existing {
        Some(loc) => {
            ditto
                .store()
                .execute_v2((
                    "UPDATE locations SET lat=:lat, lon=:lon WHERE _id=:id",
                    serde_json::json!({ "lat": lat, "lon": lon, "id": loc.id }),
                ))
                .await?;
            tracing::debug!(uid = %loc.uid, %lat, %lon, "updated existing location");
        }
        None => {
            let new_loc = LocationItem::new(uid, lat, lon);
            ditto
                .store()
                .execute_v2((
                    "INSERT INTO locations DOCUMENTS (:location)",
                    serde_json::json!({ "location": new_loc }),
                ))
                .await?;
            tracing::debug!("inserted new location from CoT");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cot_entity_alpha() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><event version="2.0" uid="Entity-Alpha" type="a-f-G-U-C" time="2026-05-14T14:29:15.93Z" start="2026-05-14T14:29:15.93Z" stale="2026-05-14T14:30:30.93Z" how="h-e" access="Undefined"><point lat="30.9223234883301" lon="-85.6955352795799" hae="100" ce="10" le="10"/><detail></detail></event>"#;
        let (uid, lat, lon) = parse_cot(xml).unwrap();
        assert_eq!(uid, "Entity-Alpha");
        assert!((lat - 30.9223234883301).abs() < 1e-9);
        assert!((lon - -85.6955352795799).abs() < 1e-9);
    }

    #[test]
    fn test_parse_cot_negative_coords() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><event version="2.0" uid="Entity-Charlie" type="a-f-G-U-C" time="2026-05-14T14:29:15.93Z" start="2026-05-14T14:29:15.93Z" stale="2026-05-14T14:30:30.93Z" how="h-e" access="Undefined"><point lat="30.9211846201167" lon="-85.6934623172988" hae="100" ce="10" le="10"/><detail></detail></event>"#;
        let (uid, _, lon) = parse_cot(xml).unwrap();
        assert_eq!(uid, "Entity-Charlie");
        assert!(lon < 0.0);
    }

    #[test]
    fn test_parse_cot_missing_point() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><event version="2.0" uid="No-Point"></event>"#;
        assert!(parse_cot(xml).is_none());
    }

    #[test]
    fn test_format_cot_roundtrip() {
        let xml = format_cot("Test-Entity", 30.9223, -85.6955);
        let (uid, lat, lon) = parse_cot(&xml).unwrap();
        assert_eq!(uid, "Test-Entity");
        assert!((lat - 30.9223).abs() < 1e-6);
        assert!((lon - -85.6955).abs() < 1e-6);
    }
}
