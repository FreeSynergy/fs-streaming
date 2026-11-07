//! A wrong key and an unknown stream do the same reads, so time does not tell them apart.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use fs_objects::traits::UnloggedObjects;
use fs_objects::{ObjectId, StoredObject, TursoStore};
use fs_streaming_core::{Gate, Repo, StreamRow, StreamService, TargetRow, keys};

/// A real store that counts its reads.
struct Counting(TursoStore, AtomicUsize);

#[async_trait]
impl UnloggedObjects for Counting {
    async fn put_unlogged(&self, o: StoredObject) -> fs_objects::Result<()> {
        self.0.put_unlogged(o).await
    }
    async fn read_unlogged(&self, id: ObjectId) -> fs_objects::Result<Option<StoredObject>> {
        self.1.fetch_add(1, Ordering::SeqCst);
        self.0.read_unlogged(id).await
    }
    async fn delete_unlogged(&self, id: ObjectId) -> fs_objects::Result<()> {
        self.0.delete_unlogged(id).await
    }
}

#[tokio::test]
async fn same_reads_for_a_wrong_key_an_unknown_stream_and_a_right_key() {
    let store = Arc::new(Counting(
        TursoStore::open_in_memory().await.unwrap(),
        AtomicUsize::new(0),
    ));
    let repo = Repo::new(store.clone());
    let targets = (0..50)
        .map(|i| TargetRow {
            id: ObjectId::new(),
            name: format!("t{i}"),
            url: "rtmp://h/a".into(),
            key: Some("k".into()),
            enabled: true,
        })
        .collect();
    let row = StreamRow {
        id: ObjectId::new(),
        name: "s".into(),
        key_hash: keys::hash("right"),
        recording: false,
        targets,
    };
    repo.put(&row).await.unwrap();
    let service = StreamService::new(store.clone());
    let reads = |before: usize| store.1.load(Ordering::SeqCst) - before;
    let before = store.1.load(Ordering::SeqCst);
    assert!(service.authorize(row.id, "wrong").await.is_none());
    let wrong = reads(before);
    let before = store.1.load(Ordering::SeqCst);
    assert!(service.authorize(ObjectId::new(), "right").await.is_none());
    let unknown = reads(before);
    let before = store.1.load(Ordering::SeqCst);
    assert!(service.authorize(row.id, "right").await.is_some());
    let right = reads(before);
    assert_eq!(
        (wrong, unknown),
        (right, right),
        "the same reads in every case"
    );
}
