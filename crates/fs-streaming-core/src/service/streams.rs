//! The `Streams` contract on `StreamService`: the stream-level methods.
//!
//! Pattern: Adapter — each trait method calls the inherent method of the same concern, so callers
//! with and without the trait agree. Refusals come from the state functions, not from here.

use async_trait::async_trait;
use fs_objects::ObjectId;

use super::handle::StreamService;
use crate::error::{Error, Result};
use crate::keys;
use crate::model::{CheckResult, Created, Cursor, Page, Status, Stream, Target};
use crate::repo::StreamRow;
use crate::secret::Secret;
use crate::state::StreamEvent;
use crate::traits::{Event, Streams};

const PAGE: usize = 100;

fn clean_name(name: &str) -> Result<String> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 200 || name.chars().any(char::is_control) {
        return Err(Error::InvalidInput(
            "the name must be 1 to 200 characters without control characters",
        ));
    }
    Ok(name.to_string())
}

#[async_trait]
impl Streams for StreamService {
    async fn get(&self, id: ObjectId) -> Result<Option<Stream>> {
        Ok(self.repo.get(id).await?.map(|r| self.stream_view(&r)))
    }

    async fn list(&self, cursor: Option<&Cursor>) -> Result<Page<Stream>> {
        let start = cursor.and_then(|c| c.0.parse::<usize>().ok()).unwrap_or(0);
        let all = self.repo.all().await?;
        let items: Vec<_> = all
            .iter()
            .skip(start)
            .take(PAGE)
            .map(|r| self.stream_view(r))
            .collect();
        let end = start + items.len();
        let next = (end < all.len()).then(|| Cursor(end.to_string()));
        Ok(Page { items, next })
    }

    async fn status(&self, id: ObjectId) -> Result<Status> {
        self.status_inner(id).await
    }

    async fn targets(&self, stream: ObjectId) -> Result<Vec<Target>> {
        let row = self.row(stream).await?;
        Ok(row
            .targets
            .iter()
            .map(|t| self.target_view(stream, t))
            .collect())
    }

    async fn create(&self, name: &str) -> Result<Created> {
        let name = clean_name(name)?;
        self.relay()?;
        let key = keys::generate()?;
        let row = StreamRow {
            id: ObjectId::new(),
            name,
            key_hash: keys::hash(key.expose()),
            recording: false,
            targets: vec![],
        };
        {
            let _w = self.repo.lock().await;
            self.repo.put(&row).await?;
        }
        let stream = self.stream_view(&row);
        self.emit(Event::Stream(stream.clone()));
        Ok(Created {
            ingest_url: stream.ingest_url.clone(),
            stream,
            publisher_key: key,
        })
    }

    async fn rotate_key(&self, id: ObjectId) -> Result<Secret> {
        let relay = self.relay()?.clone();
        let key = keys::generate()?;
        {
            let _w = self.repo.lock().await;
            let mut row = self.row(id).await?;
            row.key_hash = keys::hash(key.expose());
            self.repo.put(&row).await?;
        }
        let seen = self.generation(id);
        relay.stop_sender(id).await;
        self.finish_sender(id, Some(seen)).await;
        Ok(key)
    }

    async fn rename(&self, id: ObjectId, name: &str) -> Result<Stream> {
        let name = clean_name(name)?;
        let _w = self.repo.lock().await;
        let mut row = self.row(id).await?;
        row.name = name;
        self.repo.put(&row).await?;
        let view = self.stream_view(&row);
        self.emit(Event::Stream(view.clone()));
        Ok(view)
    }

    async fn delete(&self, id: ObjectId) -> Result<()> {
        let relay = self.relay()?.clone();
        let _w = self.repo.lock().await;
        let row = self.row(id).await?;
        if let Some(rt) = self.rt().get_mut(&id) {
            rt.apply(StreamEvent::DeleteAsked).map_err(Error::Refused)?;
        }
        relay.forget(id).await;
        self.repo.delete(id).await?;
        self.rt().remove(&id);
        for t in &row.targets {
            self.emit(Event::Removed(t.id));
        }
        self.emit(Event::Removed(id));
        Ok(())
    }

    async fn stop(&self, id: ObjectId) -> Result<Stream> {
        let row = self.row(id).await?;
        self.cancel_check(id);
        let seen = self.generation(id);
        self.relay()?.stop_sender(id).await;
        self.finish_sender(id, Some(seen)).await;
        self.finish_check(id).await;
        Ok(self.stream_view(&row))
    }

    async fn set_recording(&self, id: ObjectId, on: bool) -> Result<Stream> {
        let relay = self.relay()?.clone();
        if on && !relay.can_record() {
            return crate::unsupported::not_possible("recording.start");
        }
        let row = {
            let _w = self.repo.lock().await;
            let mut row = self.row(id).await?;
            row.recording = on;
            self.repo.put(&row).await?;
            row
        };
        if self
            .rt()
            .get(&id)
            .is_some_and(super::runtime::StreamRt::is_live)
        {
            relay.set_recording(id, on).await;
        }
        let view = self.stream_view(&row);
        self.emit(Event::Stream(view.clone()));
        Ok(view)
    }

    async fn add_target(
        &self,
        stream: ObjectId,
        name: &str,
        url: &str,
        key: &str,
    ) -> Result<Target> {
        self.add_target_inner(stream, name, url, key).await
    }
    async fn edit_target(&self, id: ObjectId, name: &str, url: &str) -> Result<Target> {
        self.edit_target_inner(id, name, url).await
    }
    async fn set_target_key(&self, id: ObjectId, key: &str) -> Result<Target> {
        self.set_target_key_inner(id, key).await
    }
    async fn set_target_enabled(&self, id: ObjectId, enabled: bool) -> Result<Target> {
        self.set_target_enabled_inner(id, enabled).await
    }
    async fn remove_target(&self, id: ObjectId) -> Result<()> {
        self.remove_target_inner(id).await
    }
    async fn check(&self, stream: ObjectId) -> Result<Vec<CheckResult>> {
        self.check_inner(stream).await
    }
}
