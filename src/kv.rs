use crate::util::{bound_copy, bound_map};
use crate::{Entry, FeedStore};
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

pub struct KvFeedStore<E: Entry> {
    // db: sled::Db,
    /// (FeedId, Seq) => Entry
    by_feedseq: sled::Tree,

    /// EntryId => (FeedId, Seq)
    by_entryid: sled::Tree,

    coder: bincode::Config,
    _entry_type: PhantomData<E>,
}

impl<E: Entry> KvFeedStore<E> {
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
            _entry_type: PhantomData,
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

    fn deserialize<'de, T: Deserialize<'de>>(&self, b: &'de [u8]) -> Result<T> {
        self.coder.deserialize(b).context(Coder)
    }
}

impl<E: Entry> FeedStore<E> for KvFeedStore<E> {
    type Error = Error;

    fn append(&self, feed_id: &E::FeedId, entry: &E) -> Result<()> {
        let (id, seq) = entry.id_seq();

        let feedseq = self.serialize(&(feed_id, seq))?;

        self.by_feedseq
            .insert(&feedseq, self.serialize(entry)?)
            .context(Sled)?;

        self.by_entryid
            .insert(self.serialize(&id)?, feedseq)
            .context(Sled)?;

        Ok(())
    }

    fn append_all<I: Iterator<Item = E>>(
        &self,
        feed_id: &E::FeedId,
        mut entries: I,
    ) -> Result<(), Self::Error> {
        // The only allocation we can avoid on each iteration is the entry_id buffer,
        // so this might not be worth the hassle.

        let mut entry_id = vec![];

        while let Some(e) = entries.next() {
            let (id, seq) = e.id_seq();

            let feedseq = self.serialize(&(feed_id, seq))?;

            self.by_feedseq
                .insert(&feedseq, self.serialize(&e)?)
                .context(Sled)?;

            entry_id.clear();
            self.coder
                .serialize_into(&mut entry_id, &id)
                .context(Coder)?;

            self.by_entryid.insert(&entry_id, feedseq).context(Sled)?;
        }
        Ok(())
    }

    fn get_entry_by_id(&self, entry_id: &E::Id) -> Result<Option<E>> {
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

    fn get_entry_by_seq(&self, feed_id: &E::FeedId, seq: E::Seq) -> Result<Option<E>> {
        self.by_feedseq
            .get(self.serialize(&(feed_id, seq))?)
            .context(Sled)
            .transpose() // Option<Result<Ivec>>
            .map(|r| r.and_then(|e| self.deserialize(&e)))
            .transpose()
    }

    fn get_entries_in_range<R>(
        &self,
        feed_id: &E::FeedId,
        range: R,
    ) -> Box<dyn Iterator<Item = Result<E>>>
    where
        R: RangeBounds<E::Seq>,
    {
        let f = |seq| self.serialize(&(feed_id, seq)).unwrap();

        let start = match bound_copy(range.start_bound()) {
            Bound::Unbounded => Bound::Included(E::Seq::zero()),
            b => b,
        };
        let end = match bound_copy(range.end_bound()) {
            Bound::Unbounded => Bound::Included(E::Seq::max_value()),
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

    fn get_latest_entry(&self, feed_id: &E::FeedId) -> Result<Option<E>> {
        match self
            .by_feedseq
            .get_lt(self.serialize(&(feed_id, E::Seq::max_value()))?)
            .context(Sled)
        {
            Ok(Some((k, v))) => {
                let (id, _seq) = self.deserialize::<(E::FeedId, E::Seq)>(&k)?;
                if &id == feed_id {
                    Some(self.deserialize::<E>(&v)).transpose()
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
    impl Entry for CoolEntry {
        type Id = [u8; 32];
        type Seq = u32;
        type FeedId = String;

        fn id_seq(&self) -> (Self::Id, Self::Seq) {
            (self.id, self.seq)
        }
    }

    #[test]
    fn basic() {
        let db = KvFeedStore::<CoolEntry>::temp().unwrap();

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

        assert_eq!(&a0, &db.get_entry_by_id(&a0.id).unwrap().unwrap());
        assert_eq!(&a1, &db.get_entry_by_id(&a1.id).unwrap().unwrap());
        assert_eq!(&b0, &db.get_entry_by_id(&b0.id).unwrap().unwrap());
        assert_eq!(&b1, &db.get_entry_by_id(&b1.id).unwrap().unwrap());

        let a2 = ann.new_entry("hooray");
        assert!(&db.get_entry_by_id(&a2.id).unwrap().is_none());
        assert!(db.get_entry_by_seq(&ann.id, 2).unwrap().is_none());
        db.append(&ann.id, &a2).unwrap();
        assert_eq!(&a2, &db.get_entry_by_seq(&ann.id, 2).unwrap().unwrap());
        assert_eq!(&a2, &db.get_entry_by_id(&a2.id).unwrap().unwrap());

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
        let db = KvFeedStore::<CoolEntry>::temp().unwrap();
        let mut ann = CoolFeed::new("ann");

        let range = 0..=256;
        for _ in range.clone() {
            let e = ann.new_entry("hi");
            db.append(&ann.id, &e).unwrap();
        }

        let mut iter = db.get_entries_in_range(&ann.id, range.clone());
        for i in range.clone() {
            assert_eq!(iter.next().unwrap().unwrap().seq, i);
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn append_iter() {
        let db = KvFeedStore::<CoolEntry>::temp().unwrap();
        let mut ann = CoolFeed::new("ann");

        let id = ann.id.clone();
        let iter = std::iter::from_fn(move || {
            let e = ann.new_entry("hello");
            if e.seq > 50 {
                None
            } else {
                Some(e)
            }
        });

        db.append_all(&id, iter).unwrap();
        let mut iter = db.get_entries_in_range(&id, ..);
        for i in 0..=50 {
            let e = iter.next().unwrap().unwrap();
            assert_eq!(e.seq, i);
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn generic() {
        let db = KvFeedStore::<CoolEntry>::temp().unwrap();
        use_feedstore(&db).unwrap();
    }

    fn use_feedstore<F: FeedStore<CoolEntry>>(db: &F) -> Result<[u8; 32], F::Error> {
        let mut ann = CoolFeed::new("ann");
        let e = ann.new_entry("hi");
        db.append(&ann.id, &e).unwrap();
        Ok(e.id)
    }
}
