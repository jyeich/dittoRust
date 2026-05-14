use anyhow::{Context, Result};
use dittolive_ditto::Ditto;
use quick_xml::events::Event as XmlEvent;
use quick_xml::Reader;
use tokio::net::UdpSocket;

use crate::tui::todolist::LocationItem;

/// Bind a UDP socket on `port` and process incoming CoT (Cursor on Target) XML messages,
/// upserting each entity into the Ditto `locations` collection by uid.
pub async fn run_udp_listener(ditto: Ditto, port: u16) -> Result<()> {
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

/// Parse a CoT XML message and return `(uid, lat, lon)`.
/// Extracts `uid` from `<event uid="...">` and `lat`/`lon` from `<point lat="..." lon="...">`.
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
                                uid = std::str::from_utf8(&attr.value)
                                    .ok()
                                    .map(str::to_owned);
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
async fn upsert_location(ditto: &Ditto, uid: String, lat: f64, lon: f64) -> Result<()> {
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
    use super::parse_cot;

    #[test]
    fn test_parse_cot_entity_alpha() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><event version="2.0" uid="Entity-Alpha" type="a-f-G-U-C" time="2026-05-14T14:29:15.93Z" start="2026-05-14T14:29:15.93Z" stale="2026-05-14T14:30:30.93Z" how="h-e" access="Undefined"><point lat="30.9223234883301" lon="-85.6955352795799" hae="100" ce="10" le="10"/><detail></detail></event>"#;
        let result = parse_cot(xml);
        assert!(result.is_some());
        let (uid, lat, lon) = result.unwrap();
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
}
