use num::{Bounded, Zero};
use serde::{de::DeserializeOwned, Serialize};
use std::ops::RangeBounds;

mod kv;
pub use kv::*;
pub mod util;

pub trait Feed {
    type Id: PartialEq + Serialize + DeserializeOwned;
    type Entry: FeedEntry;
}

pub trait FeedEntry: Serialize + DeserializeOwned {
    type Id: Serialize + DeserializeOwned;
    type Seq: Copy + Bounded + Zero + Serialize + DeserializeOwned;

    fn id(&self) -> Self::Id;
    fn seq(&self) -> Self::Seq;
}

pub trait FeedStore<F>
where
    F: Feed,
{
    type Error;

    fn append(&self, feed_id: &F::Id, entry: &F::Entry) -> Result<(), Self::Error>;

    fn append_all<I: Iterator<Item = F::Entry>>(
        &self,
        feed_id: &F::Id,
        mut entries: I,
    ) -> Result<(), Self::Error> {
        while let Some(e) = entries.next() {
            self.append(feed_id, &e)?;
        }
        Ok(())
    }

    fn get_entry_by_id(
        &self,
        id: &<F::Entry as FeedEntry>::Id,
    ) -> Result<Option<F::Entry>, Self::Error>;

    fn get_entry_by_seq(
        &self,
        feed_id: &F::Id,
        seq: <F::Entry as FeedEntry>::Seq,
    ) -> Result<Option<F::Entry>, Self::Error>;

    fn get_entries_in_range<R>(
        &self,
        feed_id: &F::Id,
        range: R,
    ) -> Box<dyn Iterator<Item = Result<F::Entry, Self::Error>>>
    where
        R: RangeBounds<<F::Entry as FeedEntry>::Seq>;

    fn get_latest_entry(&self, feed_id: &F::Id) -> Result<Option<F::Entry>, Self::Error>;
}
