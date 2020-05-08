use num::{Bounded, Zero};
use serde::{de::DeserializeOwned, Serialize};
use std::ops::RangeBounds;

mod kv;
pub use kv::*;
pub mod util;

pub trait Entry: Serialize + DeserializeOwned {
    type Id: Serialize + DeserializeOwned;
    type Seq: Copy + Bounded + Zero + Serialize + DeserializeOwned;
    type FeedId: PartialEq + Serialize + DeserializeOwned;

    fn id_seq(&self) -> (Self::Id, Self::Seq);
}

pub trait FeedStore<E: Entry> {
    type Error: std::error::Error;

    fn append(&self, feed_id: &E::FeedId, entry: &E) -> Result<(), Self::Error>;

    fn append_all<I: Iterator<Item = E>>(
        &self,
        feed_id: &E::FeedId,
        mut entries: I,
    ) -> Result<(), Self::Error> {
        while let Some(e) = entries.next() {
            self.append(feed_id, &e)?;
        }
        Ok(())
    }

    fn get_entry_by_id(&self, id: &E::Id) -> Result<Option<E>, Self::Error>;

    fn get_entry_by_seq(&self, feed_id: &E::FeedId, seq: E::Seq) -> Result<Option<E>, Self::Error>;

    fn get_entries_in_range<R>(
        &self,
        feed_id: &E::FeedId,
        range: R,
    ) -> Box<dyn Iterator<Item = Result<E, Self::Error>>>
    where
        R: RangeBounds<E::Seq>;

    fn get_latest_entry(&self, feed_id: &E::FeedId) -> Result<Option<E>, Self::Error>;
}
