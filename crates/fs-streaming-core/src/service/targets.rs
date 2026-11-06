//! Targets: add, edit, key, switch, remove.
//!
//! Pattern: Command-like methods over the repo — each reads the row under the write lock, applies the
//! rule (address check, key rule, refusal while sending), writes, then tells the relay and the feed.
//! A key never leaves this file except into the row and the relay's `TargetSpec`.

use fs_store::ObjectId;

use super::handle::StreamService;
use crate::error::{Error, Result};
use crate::keys;
use crate::model::{Target, TargetState};
use crate::repo::{StreamRow, TargetRow};
use crate::state::{Refusal, TargetEvent, next_target};
use crate::traits::{Event, TargetSpec};
use crate::url::TargetUrl;

fn clean_key(key: &str) -> Result<Option<String>> {
    if key.is_empty() {
        return Ok(None);
    }
    if !keys::is_sendable(key) {
        return Err(Error::InvalidInput("the key is not acceptable"));
    }
    Ok(Some(key.to_string()))
}

impl StreamService {
    pub(super) fn spec(row: &TargetRow) -> Result<TargetSpec> {
        Ok(TargetSpec {
            id: row.id,
            url: TargetUrl::parse(&row.url)?,
            key: row.secret(),
        })
    }

    /// Set a target's runtime state through the state function, and return the new view.
    pub(super) fn step_target(&self, stream: &StreamRow, i: usize, ev: TargetEvent) -> Target {
        let t = &stream.targets[i];
        let current = self.target_view(stream.id, t).state;
        let state = next_target(current, ev);
        let mut guard = self.rt();
        let rt = guard.entry(stream.id).or_default();
        let note = rt.targets.get(&t.id).and_then(|k| k.note);
        rt.targets
            .insert(t.id, super::runtime::TargetRt { state, note });
        drop(guard);
        self.target_view(stream.id, t)
    }

    fn live(&self, stream: ObjectId) -> bool {
        self.rt()
            .get(&stream)
            .is_some_and(super::runtime::StreamRt::is_live)
    }

    async fn tell_relay(&self, row: &StreamRow, i: usize) -> Result<()> {
        if self.live(row.id) {
            let t = &row.targets[i];
            self.relay()?
                .apply_target(row.id, Self::spec(t)?, t.enabled)
                .await;
        }
        Ok(())
    }

    pub(super) async fn add_target_inner(
        &self,
        stream: ObjectId,
        name: &str,
        url: &str,
        key: &str,
    ) -> Result<Target> {
        let url = TargetUrl::parse(url)?;
        let key = clean_key(key)?;
        self.refuse_own(&url).await?;
        let name = name.trim();
        if name.is_empty() || name.chars().any(char::is_control) {
            return Err(Error::InvalidInput("the name must not be empty"));
        }
        let _w = self.repo.lock().await;
        let mut row = self.row(stream).await?;
        row.targets.push(TargetRow {
            id: ObjectId::new(),
            name: name.into(),
            url: url.to_string(),
            key,
            enabled: true,
        });
        self.repo.put(&row).await?;
        let i = row.targets.len() - 1;
        let live = self.live(stream);
        let view = self.step_target(&row, i, TargetEvent::Enabled { live });
        self.tell_relay(&row, i).await?;
        self.emit(Event::Target(view.clone()));
        Ok(view)
    }

    async fn refuse_own(&self, url: &TargetUrl) -> Result<()> {
        match self.relay() {
            Ok(r) if r.is_own_address(url.host(), url.port()).await => {
                Err(Error::InvalidInput("the address is this relay's own"))
            }
            _ => Ok(()),
        }
    }

    async fn find(&self, id: ObjectId) -> Result<(StreamRow, usize)> {
        self.repo
            .find_target(id)
            .await?
            .ok_or(Error::NotFound("target"))
    }

    pub(super) async fn edit_target_inner(
        &self,
        id: ObjectId,
        name: &str,
        url: &str,
    ) -> Result<Target> {
        let new = TargetUrl::parse(url)?;
        self.refuse_own(&new).await?;
        let _w = self.repo.lock().await;
        let (mut row, i) = self.find(id).await?;
        if self.target_view(row.id, &row.targets[i]).state == TargetState::Sending {
            return Err(Error::Refused(Refusal::TargetSending));
        }
        let old = TargetUrl::parse(&row.targets[i].url)?;
        let t = &mut row.targets[i];
        if !old.keeps_key_for(&new) {
            t.key = None;
        }
        t.url = new.to_string();
        if !name.trim().is_empty() {
            t.name = name.trim().into();
        }
        self.repo.put(&row).await?;
        let live = self.live(row.id);
        let view = self.step_target(&row, i, TargetEvent::Edited { live });
        self.tell_relay(&row, i).await?;
        self.emit(Event::Target(view.clone()));
        Ok(view)
    }

    pub(super) async fn set_target_key_inner(&self, id: ObjectId, key: &str) -> Result<Target> {
        let key = clean_key(key)?;
        let _w = self.repo.lock().await;
        let (mut row, i) = self.find(id).await?;
        row.targets[i].key = key;
        self.repo.put(&row).await?;
        let live = self.live(row.id);
        let view = self.step_target(&row, i, TargetEvent::Edited { live });
        self.tell_relay(&row, i).await?;
        self.emit(Event::Target(view.clone()));
        Ok(view)
    }

    pub(super) async fn set_target_enabled_inner(
        &self,
        id: ObjectId,
        enabled: bool,
    ) -> Result<Target> {
        let _w = self.repo.lock().await;
        let (mut row, i) = self.find(id).await?;
        row.targets[i].enabled = enabled;
        self.repo.put(&row).await?;
        let live = self.live(row.id);
        let ev = if enabled {
            TargetEvent::Enabled { live }
        } else {
            TargetEvent::Disabled
        };
        let view = self.step_target(&row, i, ev);
        self.tell_relay(&row, i).await?;
        self.emit(Event::Target(view.clone()));
        Ok(view)
    }

    pub(super) async fn remove_target_inner(&self, id: ObjectId) -> Result<()> {
        let _w = self.repo.lock().await;
        let (mut row, i) = self.find(id).await?;
        row.targets.remove(i);
        self.repo.put(&row).await?;
        if let Some(rt) = self.rt().get_mut(&row.id) {
            rt.targets.remove(&id);
        }
        if let Ok(relay) = self.relay() {
            relay.drop_target(id).await;
        }
        self.emit(Event::Removed(id));
        Ok(())
    }
}
