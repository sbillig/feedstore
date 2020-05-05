use crate::util::{bound_copy, bound_map};
use crate::{Feed, FeedEntry, FeedStore};
use bincode;
use num::{Bounded, Zero};
use serde::{Deserialize, Serialize};
use sled;
use snafu::{ResultExt, Snafu};
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::path::Path;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("`sled` error: {}", source))]
    Sled { source: sled::Error },

    #[snafu(display("`bincode` error: {}", source))]
    Coder { source: bincode::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct KvFeedStore<F>
where
    F: Feed,
{
    // db: sled::Db,
    /// (FeedId, Seq) => Entry
    by_feedseq: sled::Tree,

    /// EntryId => (FeedId, Seq)
    by_entryid: sled::Tree,

    coder: bincode::Config,
    _feed_type: PhantomData<F>,
}

impl<F> KvFeedStore<F>
where
    F: Feed,
{
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(Self::default_config().path(path).create_new(true))
    }

    pub fn temp() -> Result<Self> {
        Self::open_with_config(Self::default_config().temporary(true))
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(Self::default_config().path(path))
    }

    fn init(db: sled::Db) -> Result<Self> {
        let by_feedseq = db.open_tree("by_feedseq").context(Sled)?;
        let by_entryid = db.open_tree("by_entryid").context(Sled)?;

        let mut coder = bincode::config();
        coder.big_endian();

        Ok(KvFeedStore {
            by_feedseq,
            by_entryid,
            coder,
            _feed_type: PhantomData,
        })
    }

    pub fn open_with_config(config: sled::Config) -> Result<Self> {
        Self::init(config.open().context(Sled)?)
    }

    pub fn default_config() -> sled::Config {
        // segment_size: 2 << 22, // 8mb
        // read_only: false,
        // create_new: false,
        // cache_capacity: 1024 * 1024 * 1024, // 1gb
        // use_compression: false,
        // compression_factor: 5,
        // flush_every_ms: Some(500),
        // snapshot_after_ops: 1_000_000,
        // snapshot_path: None,
        // segment_cleanup_threshold: 40,
        // segment_cleanup_skew: 10,
        // temporary: false,
        // segment_mode: SegmentMode::Gc,
        // print_profile_on_drop: false,
        // idgen_persist_interval: 1_000_000,
        sled::Config::default()
    }

    fn serialize<T: ?Sized + Serialize>(&self, t: &T) -> Result<Vec<u8>> {
        self.coder.serialize(t).context(Coder)
    }

    fn deserialize<'a, T: Deserialize<'a>>(&self, b: &'a [u8]) -> Result<T> {
        self.coder.deserialize(b).context(Coder)
    }
}

impl<F: Feed> FeedStore<F> for KvFeedStore<F> {
    type Error = Error;

    fn append(&self, feed_id: &F::Id, entry: &F::Entry) -> Result<()> {
        let feedseq = self.serialize(&(feed_id, entry.seq()))?;

        self.by_feedseq
            .insert(&feedseq, self.serialize(entry)?)
            .context(Sled)?;

        self.by_entryid
            .insert(self.serialize(&entry.id())?, feedseq)
            .context(Sled)?;

        Ok(())
    }

    fn append_all<I: Iterator<Item = F::Entry>>(
        &self,
        feed_id: &F::Id,
        mut entries: I,
    ) -> Result<(), Self::Error> {
        // The only allocation we can avoid on each iteration is the entry_id buffer,
        // so this might not be worth the hassle.

        let mut entry_id = vec![];

        while let Some(e) = entries.next() {
            let feedseq = self.serialize(&(feed_id, e.seq()))?;

            self.by_feedseq
                .insert(&feedseq, self.serialize(&e)?)
                .context(Sled)?;

            entry_id.clear();
            self.coder
                .serialize_into(&mut entry_id, &e.id())
                .context(Coder)?;

            self.by_entryid.insert(&entry_id, feedseq).context(Sled)?;
        }
        Ok(())
    }

    fn get_entry_by_id(&self, entry_id: &<F::Entry as FeedEntry>::Id) -> Result<Option<F::Entry>> {
        if let Some(feedseq) = self
            .by_entryid
            .get(self.serialize(entry_id)?)
            .context(Sled)?
        {
            if let Some(e) = self.by_feedseq.get(&feedseq).context(Sled)? {
                return Ok(Some(self.deserialize(&e)?));
            }
        }
        Ok(None)
    }

    fn get_entry_by_seq(
        &self,
        feed_id: &F::Id,
        seq: <F::Entry as FeedEntry>::Seq,
    ) -> Result<Option<F::Entry>> {
        self.by_feedseq
            .get(self.serialize(&(feed_id, seq))?)
            .context(Sled)
            .transpose() // Option<Result<Ivec>>
            .map(|r| r.and_then(|e| self.deserialize(&e)))
            .transpose()
    }

    fn get_entries_in_range<R>(
        &self,
        feed_id: &F::Id,
        range: R,
    ) -> Box<dyn Iterator<Item = Result<F::Entry>>>
    where
        R: RangeBounds<<F::Entry as FeedEntry>::Seq>,
    {
        let f = |seq| self.serialize(&(feed_id, seq)).unwrap();

        let start = match bound_copy(range.start_bound()) {
            Bound::Unbounded => Bound::Included(<F::Entry as FeedEntry>::Seq::zero()),
            b => b,
        };
        let end = match bound_copy(range.end_bound()) {
            Bound::Unbounded => Bound::Included(<F::Entry as FeedEntry>::Seq::max_value()),
            b => b,
        };

        let r = (bound_map(start, f), bound_map(end, f));

        Box::new(self.by_feedseq.range(r).map(|r| {
            r.context(Sled).and_then(|(_k, v)| {
                // TODO: better way to do this??
                let mut conf = bincode::config();
                conf.big_endian();
                conf.deserialize(&v).context(Coder)
            })
        }))
    }

    fn get_latest_entry(&self, feed_id: &F::Id) -> Result<Option<F::Entry>> {
        match self
            .by_feedseq
            .get_lt(self.serialize(&(feed_id, <F::Entry as FeedEntry>::Seq::max_value()))?)
            .context(Sled)
        {
            Ok(Some((k, v))) => {
                let (id, _seq) = self.deserialize::<(F::Id, <F::Entry as FeedEntry>::Seq)>(&k)?;
                if &id == feed_id {
                    Some(self.deserialize::<F::Entry>(&v)).transpose()
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blake3::hash;
    use serde::{Deserialize, Serialize};

    struct CoolFeed {
        id: String,
        seq: u32,
    }
    impl CoolFeed {
        fn new(id: &str) -> CoolFeed {
            CoolFeed {
                id: id.to_string(),
                seq: 0,
            }
        }
        fn new_entry(&mut self, body: &str) -> CoolEntry {
            let seq = self.seq;
            self.seq += 1;
            CoolEntry {
                id: *hash(&bincode::serialize(&(&self.id, seq, body)).unwrap()).as_bytes(),
                seq,
                body: body.to_string(),
            }
        }
    }

    impl Feed for CoolFeed {
        type Id = String;
        type Entry = CoolEntry;
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct CoolEntry {
        id: [u8; 32],
        seq: u32,
        body: String,
    }
    impl PartialEq<CoolEntry> for CoolEntry {
        fn eq(&self, e: &CoolEntry) -> bool {
            self.seq == e.seq && self.id == e.id && self.body == e.body
        }
    }
    impl FeedEntry for CoolEntry {
        type Id = [u8; 32];
        type Seq = u32;

        fn id(&self) -> Self::Id {
            self.id
        }
        fn seq(&self) -> Self::Seq {
            self.seq
        }
    }

    #[test]
    fn basic() {
        let db = KvFeedStore::<CoolFeed>::temp().unwrap();

        let mut ann = CoolFeed::new("ann");
        let mut bob = CoolFeed::new("bob");

        let a0 = ann.new_entry("hello bob");
        let b0 = bob.new_entry("hello ann");
        let a1 = ann.new_entry("how are you?");
        let b1 = bob.new_entry("oh just swell, thanks");

        db.append(&ann.id, &a0).unwrap();
        db.append(&bob.id, &b0).unwrap();
        db.append(&ann.id, &a1).unwrap();
        db.append(&bob.id, &b1).unwrap();

        assert_eq!(&a0, &db.get_entry_by_seq(&ann.id, 0).unwrap().unwrap());
        assert_eq!(&a1, &db.get_entry_by_seq(&ann.id, 1).unwrap().unwrap());
        assert_eq!(&b0, &db.get_entry_by_seq(&bob.id, 0).unwrap().unwrap());
        assert_eq!(&b1, &db.get_entry_by_seq(&bob.id, 1).unwrap().unwrap());

        assert_eq!(&a0, &db.get_entry_by_id(&a0.id()).unwrap().unwrap());
        assert_eq!(&a1, &db.get_entry_by_id(&a1.id()).unwrap().unwrap());
        assert_eq!(&b0, &db.get_entry_by_id(&b0.id()).unwrap().unwrap());
        assert_eq!(&b1, &db.get_entry_by_id(&b1.id()).unwrap().unwrap());

        let a2 = ann.new_entry("hooray");
        assert!(&db.get_entry_by_id(&a2.id()).unwrap().is_none());
        assert!(db.get_entry_by_seq(&ann.id, 2).unwrap().is_none());
        db.append(&ann.id, &a2).unwrap();
        assert_eq!(&a2, &db.get_entry_by_seq(&ann.id, 2).unwrap().unwrap());
        assert_eq!(&a2, &db.get_entry_by_id(&a2.id()).unwrap().unwrap());

        let a3 = ann.new_entry("asdf");
        db.append(&ann.id, &a3).unwrap();

        let mut i = db.get_entries_in_range(&ann.id, 0..);
        assert_eq!(i.next().unwrap().unwrap(), a0);
        assert_eq!(i.next().unwrap().unwrap(), a1);
        assert_eq!(i.next().unwrap().unwrap(), a2);
        assert_eq!(i.next().unwrap().unwrap(), a3);
        assert!(i.next().is_none());

        let mut i = db.get_entries_in_range(&ann.id, 1..=2);
        assert_eq!(i.next().unwrap().unwrap(), a1);
        assert_eq!(i.next().unwrap().unwrap(), a2);
        assert!(i.next().is_none());

        let mut i = db.get_entries_in_range(&ann.id, 10..);
        assert!(i.next().is_none());

        let mut i = db.get_entries_in_range(&"foo".to_string(), ..);
        assert!(i.next().is_none());

        assert_eq!(a3, db.get_latest_entry(&ann.id).unwrap().unwrap());
        assert_eq!(b1, db.get_latest_entry(&bob.id).unwrap().unwrap());
        let b2 = bob.new_entry("lkjskjdf");
        db.append(&bob.id, &b2).unwrap();
        assert_eq!(b2, db.get_latest_entry(&bob.id).unwrap().unwrap());

        assert!(db.get_latest_entry(&"foo".to_string()).unwrap().is_none());
    }

    #[test]
    fn endian() {
        let db = KvFeedStore::<CoolFeed>::temp().unwrap();
        let mut ann = CoolFeed::new("ann");

        let range = 0..=256;
        for _ in range.clone() {
            let e = ann.new_entry("hi");
            db.append(&ann.id, &e).unwrap();
        }

        let mut iter = db.get_entries_in_range(&ann.id, range.clone());
        for i in range.clone() {
            assert_eq!(iter.next().unwrap().unwrap().seq(), i);
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn append_iter() {
        let db = KvFeedStore::<CoolFeed>::temp().unwrap();
        let mut ann = CoolFeed::new("ann");

        let id = ann.id.clone();
        let iter = std::iter::from_fn(move || {
            let e = ann.new_entry("hello");
            if e.seq() > 50 {
                None
            } else {
                Some(e)
            }
        });

        db.append_all(&id, iter).unwrap();
        let mut iter = db.get_entries_in_range(&id, ..);
        for i in 0..=50 {
            let e = iter.next().unwrap().unwrap();
            assert_eq!(e.seq(), i);
        }
        assert!(iter.next().is_none());
    }
}
