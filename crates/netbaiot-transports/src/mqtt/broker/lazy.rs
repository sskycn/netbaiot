//! Empty MQTT extensions occupy one pointer per connection. The shared empty
//! value is immutable; a collection is owned only after the first mutation that
//! creates work. Removing its last item returns that allocation to the allocator.
use super::*;
use std::{
    borrow::Borrow,
    ops::{Deref, DerefMut},
    sync::OnceLock,
};

pub(super) trait Empty: Default + 'static {
    fn empty() -> &'static Self;
    fn is_empty(&self) -> bool;
}

macro_rules! empty_collection {
    ($($collection:ty),+ $(,)?) => {$ (
        impl Empty for $collection {
            fn empty() -> &'static Self {
                static EMPTY: OnceLock<$collection> = OnceLock::new();
                EMPTY.get_or_init(Self::default)
            }
            fn is_empty(&self) -> bool { self.is_empty() }
        }
    )+};
}

empty_collection!(
    HashMap<String, Subscription>,
    HashMap<u16, InboundQos2State>,
    HashMap<u16, u64>,
    HashMap<u16, RetainedReservation>,
    HashMap<u16, OutboundState>,
    HashMap<u16, Arc<netbaiot_runtime::CommandProgress>>,
    HashSet<u16>,
    VecDeque<BrokerMessage>,
    VecDeque<u16>,
);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Lazy<T: Empty>(Option<Box<T>>);

impl<T: Empty> Default for Lazy<T> {
    fn default() -> Self {
        Self(None)
    }
}
impl<T: Empty> Deref for Lazy<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        match self.0.as_deref() {
            Some(owned) => owned,
            None => T::empty(),
        }
    }
}
impl<T: Empty> DerefMut for Lazy<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.get_or_insert_with(Box::default)
    }
}
impl<T: Empty> Lazy<T> {
    // Shadows the collection's clear: reconnecting an unused session must not
    // materialize an empty extension, and old network windows retain no capacity.
    pub(super) fn clear(&mut self) {
        self.0 = None;
    }
    fn release_empty(&mut self) {
        if self.0.as_deref().is_some_and(Empty::is_empty) {
            self.0 = None;
        }
    }
}
impl<T: Empty + Serialize> Serialize for Lazy<T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.deref().serialize(serializer)
    }
}
impl<T: Empty + IntoIterator> IntoIterator for Lazy<T> {
    type Item = T::Item;
    type IntoIter = T::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.0.map_or_else(T::default, |owned| *owned).into_iter()
    }
}
impl<'a, T: Empty> IntoIterator for &'a Lazy<T>
where
    &'a T: IntoIterator,
{
    type Item = <&'a T as IntoIterator>::Item;
    type IntoIter = <&'a T as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.deref().into_iter()
    }
}

impl<K: Eq + Hash, V> Lazy<HashMap<K, V>>
where
    HashMap<K, V>: Empty,
{
    pub(super) fn get_mut<Q: ?Sized + Eq + Hash>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
    {
        self.0.as_deref_mut().and_then(|map| map.get_mut(key))
    }
    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.0.iter_mut().flat_map(|map| map.values_mut())
    }
    pub(super) fn remove<Q: ?Sized + Eq + Hash>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        let removed = self.0.as_deref_mut().and_then(|map| map.remove(key));
        self.release_empty();
        removed
    }
}
impl<K: Eq + Hash> Lazy<HashSet<K>>
where
    HashSet<K>: Empty,
{
    pub(super) fn remove<Q: ?Sized + Eq + Hash>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        let removed = self.0.as_deref_mut().is_some_and(|set| set.remove(key));
        self.release_empty();
        removed
    }
}
impl<T> Lazy<VecDeque<T>>
where
    VecDeque<T>: Empty,
{
    pub(super) fn pop_front(&mut self) -> Option<T> {
        let removed = self.0.as_deref_mut().and_then(|queue| queue.pop_front());
        self.release_empty();
        removed
    }
    pub(super) fn retain(&mut self, predicate: impl FnMut(&T) -> bool) {
        if let Some(queue) = self.0.as_deref_mut() {
            queue.retain(predicate);
        }
        self.release_empty();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_extensions_read_and_reconnect_without_allocating() {
        let mut map = Lazy::<HashMap<u16, u64>>::default();
        assert!(map.is_empty());
        assert_eq!(map.get(&7), None);
        assert_eq!(map.get_mut(&7), None);
        assert_eq!(map.values_mut().count(), 0);
        assert_eq!(map.remove(&7), None);
        map.clear();
        assert!(map.0.is_none());
        assert_eq!(serde_json::to_string(&map).unwrap(), "{}");
        map.insert(7, 42);
        assert_eq!(map.get(&7), Some(&42));
        assert!(map.0.is_some());
        assert_eq!(map.remove(&7), Some(42));
        assert!(map.0.is_none());
        let mut queue = Lazy::<VecDeque<u16>>::default();
        queue.retain(|_| true);
        assert_eq!(queue.pop_front(), None);
        assert!(queue.0.is_none());
        queue.push_back(7);
        assert_eq!(queue.pop_front(), Some(7));
        assert!(queue.0.is_none());
        assert_eq!(serde_json::to_string(&queue).unwrap(), "[]");
    }
}
