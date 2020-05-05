use std::ops::Bound;

pub fn bound_map<T, U, F: FnOnce(T) -> U>(b: Bound<T>, f: F) -> Bound<U> {
    match b {
        Bound::Included(t) => Bound::Included(f(t)),
        Bound::Excluded(t) => Bound::Excluded(f(t)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub fn bound_copy<T: Copy>(b: Bound<&T>) -> Bound<T> {
    bound_map(b, |t| *t)
}
