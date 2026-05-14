use anyhow::Context;
use anyhow::Result;
use crossterm::event::Event;
use dittolive_ditto::store::StoreObserver;
use dittolive_ditto::sync::SyncSubscription;
use dittolive_ditto::Ditto;
use ratatui::prelude::*;
use ratatui::widgets::Block;
use ratatui::widgets::BorderType;
use ratatui::widgets::Clear;
use ratatui::widgets::Padding;
use ratatui::widgets::{Cell, Row, StatefulWidget, Table, TableState};
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

use crate::key;

use super::EventResult;

pub struct Todolist {
    pub ditto: Ditto,
    pub tasks_observer: Arc<StoreObserver>,
    pub tasks_rx: watch::Receiver<Vec<LocationItem>>,
    pub tasks_subscription: Arc<SyncSubscription>,
    pub websocket_url: String,
    pub client_name: Option<String>,
    pub mode: TodoMode,
    pub table_state: TableState,
}

#[derive(Debug)]
pub enum TodoMode {
    Normal,
    CreateLocation { buffer: String },
    EditLocation { id: String, buffer: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationItem {
    #[serde(rename = "_id")]
    pub id: String,
    pub uid: String,
    pub lat: f64,
    pub lon: f64,
    pub deleted: bool,
}

impl LocationItem {
    pub fn new(uid: String, lat: f64, lon: f64) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            uid,
            lat,
            lon,
            deleted: false,
        }
    }
}

impl Todolist {
    pub fn new(ditto: Ditto, websocket_url: String, client_name: Option<String>) -> Result<Self> {
        let (tasks_tx, tasks_rx) = watch::channel(Vec::new());

        let tasks_subscription = ditto
            .sync()
            .register_subscription_v2("SELECT * FROM locations")?;

        let tasks_observer = ditto.store().register_observer_v2(
            "SELECT * FROM locations WHERE deleted=false ORDER BY _id ASC",
            move |query_result| {
                let docs = query_result
                    .into_iter()
                    .flat_map(|it| it.deserialize_value::<LocationItem>().ok())
                    .collect::<Vec<_>>();
                tasks_tx.send_replace(docs);
            },
        )?;

        Ok(Self {
            ditto,
            table_state: Default::default(),
            tasks_rx,
            tasks_observer,
            tasks_subscription,
            websocket_url,
            client_name,
            mode: TodoMode::Normal,
        })
    }

    pub fn render(&mut self, area: Rect, buf: &mut Buffer) {
        self.render_todo_table(area, buf);
        self.render_location_prompt(area, buf);
    }

    fn render_todo_table(&mut self, area: Rect, buf: &mut Buffer) {
        let locations = self.tasks_rx.borrow().clone();

        let header = ["UID".bold(), "Latitude".bold(), "Longitude".bold()]
            .into_iter()
            .map(Cell::from)
            .collect::<Row>();

        let rows = locations
            .iter()
            .map(|doc| {
                [
                    Cell::from(Text::raw(doc.uid.clone())),
                    Cell::from(Text::raw(format!("{:.6}", doc.lat))),
                    Cell::from(Text::raw(format!("{:.6}", doc.lon))),
                ]
                .into_iter()
                .collect::<Row>()
            })
            .collect::<Vec<_>>();

        let sync_state = if self.ditto.is_sync_active() {
            " 🟢 Sync Active ".green()
        } else {
            " 🔴 Sync Inactive ".red()
        };
        let sync_line = [sync_state, "(s: toggle sync) ".into()]
            .into_iter()
            .collect::<Line>();

        let connection_info = if let Some(ref client_name) = self.client_name {
            format!(" {}@{} ", client_name, self.websocket_url)
        } else {
            format!(" {} ", self.websocket_url)
        };
        let connection_line = Line::raw(connection_info).cyan();

        let table = Table::new(rows, Constraint::from_percentages([30, 35, 35]))
            .header(header)
            .highlight_symbol("❯❯ ")
            .row_highlight_style(Style::new().bold().blue())
            .block(
                Block::bordered()
                    .border_type(BorderType::Rounded)
                    .title_top(Line::raw(" Locations (j↓, k↑) ").left_aligned())
                    .title_top(sync_line.right_aligned())
                    .title_bottom(
                        Line::raw(" (c: create) (d: delete) (e: edit) (q: quit) ").left_aligned(),
                    )
                    .title_bottom(connection_line.right_aligned()),
            );
        StatefulWidget::render(table, area, buf, &mut self.table_state);
    }

    fn render_location_prompt(&self, area: Rect, buf: &mut Buffer) {
        let buffer = match &self.mode {
            TodoMode::CreateLocation { buffer } => buffer,
            TodoMode::EditLocation { buffer, .. } => buffer,
            _ => return,
        };

        let space = area.inner(Margin::new(2, 2));
        Clear.render(space, buf);
        Block::bordered()
            .border_type(BorderType::Rounded)
            .title(" Enter Location (uid,lat,lon) ")
            .title_bottom(" (Esc: back) ")
            .padding(Padding::uniform(1))
            .render(space, buf);
        let space = space.inner(Margin::new(2, 2));
        Line::raw(buffer).render(space, buf);
    }

    pub async fn try_handle_event(&mut self, event: &Event) -> Result<EventResult> {
        match (&mut self.mode, event) {
            (TodoMode::Normal, key!(Char('c'))) => {
                self.mode = TodoMode::CreateLocation {
                    buffer: String::new(),
                };
            }
            (TodoMode::Normal, key!(Char('d'))) => {
                self.try_delete_location().await?;
            }
            (TodoMode::Normal, key!(Char('e'))) => {
                let selected = self
                    .table_state
                    .selected()
                    .context("failed to get selected index")?;
                let item = self
                    .tasks_rx
                    .borrow()
                    .get(selected)
                    .cloned()
                    .context("failed to get location from list")?;
                self.mode = TodoMode::EditLocation {
                    id: item.id.to_string(),
                    buffer: format!("{},{},{}", item.uid, item.lat, item.lon),
                };
            }
            (TodoMode::Normal, key!(Char('s'))) => {
                self.toggle_sync()?;
            }
            (TodoMode::CreateLocation { .. } | TodoMode::EditLocation { .. }, key!(Esc)) => {
                self.mode = TodoMode::Normal;
            }
            (TodoMode::Normal, key!(Up) | key!(Char('k'))) => {
                self.table_state.select_previous();
            }
            (TodoMode::Normal, key!(Down) | key!(Char('j'))) => {
                self.table_state.select_next();
            }
            (TodoMode::CreateLocation { buffer }, key!(Char(ch))) => {
                buffer.push(*ch);
            }
            (TodoMode::CreateLocation { buffer }, key!(Enter)) => {
                if !buffer.is_empty() {
                    let input = std::mem::take(buffer);
                    if let Some((uid, lat, lon)) = parse_uid_lat_lon(&input) {
                        self.try_create_location(uid, lat, lon).await?;
                    }
                    self.mode = TodoMode::Normal;
                }
            }
            (TodoMode::EditLocation { id, buffer }, key!(Enter)) => {
                if !buffer.is_empty() {
                    let input = std::mem::take(buffer);
                    let id = id.clone();
                    if let Some((uid, lat, lon)) = parse_uid_lat_lon(&input) {
                        self.try_edit_location(&id, uid, lat, lon).await?;
                    }
                    self.mode = TodoMode::Normal;
                }
            }
            (TodoMode::EditLocation { buffer, .. }, key!(Char(ch))) => {
                buffer.push(*ch);
            }
            (
                TodoMode::CreateLocation { buffer } | TodoMode::EditLocation { buffer, .. },
                key!(Backspace),
            ) => {
                if buffer.is_empty() {
                    self.mode = TodoMode::Normal;
                } else {
                    buffer.pop();
                }
            }
            _ => {
                return Ok(EventResult::Ignored);
            }
        }

        Ok(EventResult::Consumed)
    }

    fn toggle_sync(&mut self) -> Result<()> {
        if self.ditto.is_sync_active() {
            self.ditto.stop_sync();
        } else {
            self.ditto.start_sync()?;
        }
        Ok(())
    }

    pub async fn try_delete_location(&mut self) -> Result<()> {
        let locations = self.tasks_rx.borrow().clone();
        let index = self
            .table_state
            .selected()
            .context("failed to get selected index")?;
        let selected = locations
            .get(index)
            .cloned()
            .context("failed to find selected location")?;

        self.ditto
            .store()
            .execute_v2((
                "UPDATE locations SET deleted=true WHERE _id=:id",
                serde_json::json!({ "id": selected.id }),
            ))
            .await?;

        Ok(())
    }

    pub async fn try_create_location(&mut self, uid: String, lat: f64, lon: f64) -> Result<()> {
        let location = LocationItem::new(uid, lat, lon);
        self.ditto
            .store()
            .execute_v2((
                "INSERT INTO locations DOCUMENTS (:location)",
                serde_json::json!({ "location": location }),
            ))
            .await?;
        Ok(())
    }

    pub async fn try_edit_location(&mut self, id: &str, uid: String, lat: f64, lon: f64) -> Result<()> {
        self.ditto
            .store()
            .execute_v2((
                "UPDATE locations SET uid=:uid, lat=:lat, lon=:lon WHERE _id=:id",
                serde_json::json!({
                    "uid": uid,
                    "lat": lat,
                    "lon": lon,
                    "id": id
                }),
            ))
            .await?;
        Ok(())
    }
}

fn parse_uid_lat_lon(s: &str) -> Option<(String, f64, f64)> {
    let mut parts = s.splitn(3, ',');
    let uid = parts.next()?.trim().to_string();
    let lat = parts.next()?.trim().parse().ok()?;
    let lon = parts.next()?.trim().parse().ok()?;
    Some((uid, lat, lon))
}
