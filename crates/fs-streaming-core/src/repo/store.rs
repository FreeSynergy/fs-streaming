//! `Repo`: read and write the rows.
//!
//! Pattern: Repository — one object per stream (type `LiveStream`) plus one index object that lists
//! the stream ids, because `UnloggedObjects` has no query. Writes that read first are serialised by a
//! lock. Nothing here touches the activity log.

use std::sync::Arc;

use fs_store::traits::UnloggedObjects;
use fs_store::{Envelope, ObjectId, StoredObject};
use tokio::sync::Mutex;

use super::rows::StreamRow;
use crate::error::Result;

const STREAM_TYPE: &str = "LiveStream";
const INDEX_TYPE: &str = "LiveStreamIndex";
const CONTENT_TYPE: &str = "application/vnd.freesynergy.streaming+json";
const INDEX_ID: &str = "0194f2a0-0000-7000-8000-5354524d4958";

/// The streams of one program instance, in a private store.
pub struct Repo {
    store: Arc<dyn UnloggedObjects>,
    lock: Mutex<()>,
}

impl Repo {
    /// Rows in `store`, which must be kept apart from the hub and from logged data.
    pub fn new(store: Arc<dyn UnloggedObjects>) -> Self {
        Self {
            store,
            lock: Mutex::new(()),
        }
    }

    fn index_id() -> ObjectId {
        INDEX_ID.parse().expect("the index id is a constant UUIDv7")
    }

    async fn ids(&self) -> Result<Vec<ObjectId>> {
        let Some(obj) = self.store.read_unlogged(Self::index_id()).await? else {
            return Ok(Vec::new());
        };
        Ok(serde_json::from_value(obj.content)?)
    }

    async fn put_ids(&self, ids: &[ObjectId]) -> Result<()> {
        let mut env = Envelope::new(INDEX_TYPE, "streams");
        env.id = Self::index_id();
        let obj = StoredObject::new(env, CONTENT_TYPE, serde_json::to_value(ids)?);
        Ok(self.store.put_unlogged(obj).await?)
    }

    /// One stream row.
    pub async fn get(&self, id: ObjectId) -> Result<Option<StreamRow>> {
        if !self.ids().await?.contains(&id) {
            return Ok(None);
        }
        match self.store.read_unlogged(id).await? {
            Some(obj) => Ok(Some(serde_json::from_value(obj.content)?)),
            None => Ok(None),
        }
    }

    /// The row of `id` and the row to do the same work on: for an unknown id a stored row stands in,
    /// so a wrong key and an unknown stream cost the same reads.
    pub async fn get_or_decoy(
        &self,
        id: ObjectId,
    ) -> Result<(Option<StreamRow>, Option<StreamRow>)> {
        let ids = self.ids().await?;
        if ids.contains(&id) {
            let row = self.read_row(id).await?;
            return Ok((row.clone(), row));
        }
        let decoy = match ids.first() {
            Some(first) => self.read_row(*first).await?,
            None => None,
        };
        Ok((None, decoy))
    }

    async fn read_row(&self, id: ObjectId) -> Result<Option<StreamRow>> {
        match self.store.read_unlogged(id).await? {
            Some(obj) => Ok(Some(serde_json::from_value(obj.content)?)),
            None => Ok(None),
        }
    }

    /// All stream rows, in creation order.
    pub async fn all(&self) -> Result<Vec<StreamRow>> {
        let mut out = Vec::new();
        for id in self.ids().await? {
            if let Some(row) = self.get(id).await? {
                out.push(row);
            }
        }
        Ok(out)
    }

    /// The stream row that holds a target, and the target's place in it.
    pub async fn find_target(&self, target: ObjectId) -> Result<Option<(StreamRow, usize)>> {
        for row in self.all().await? {
            if let Some(i) = row.targets.iter().position(|t| t.id == target) {
                return Ok(Some((row, i)));
            }
        }
        Ok(None)
    }

    /// Write a row (new or changed). The caller holds [`Repo::lock`] when it read first.
    pub async fn put(&self, row: &StreamRow) -> Result<()> {
        let mut env = Envelope::new(STREAM_TYPE, row.name.as_str());
        env.id = row.id;
        let obj = StoredObject::new(env, CONTENT_TYPE, serde_json::to_value(row)?);
        self.store.put_unlogged(obj).await?;
        let mut ids = self.ids().await?;
        if !ids.contains(&row.id) {
            ids.push(row.id);
            self.put_ids(&ids).await?;
        }
        Ok(())
    }

    /// Delete a row.
    pub async fn delete(&self, id: ObjectId) -> Result<()> {
        let mut ids = self.ids().await?;
        ids.retain(|i| *i != id);
        self.put_ids(&ids).await?;
        Ok(self.store.delete_unlogged(id).await?)
    }

    /// The write lock: hold it while reading then writing.
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.lock.lock().await
    }
}
