// TODO: add cargo husky
// TODO: add errors

use std::borrow::Borrow;

struct DBService<K, V> {
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> DBService<K, V> {
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    fn put(&self, key: K, value: V) -> Result<(), ()> {
        todo!()
    }

    // todo: asref/borrow?
    fn get<Q>(&self, key: &Q) -> Result<V, ()>
    where
        K: Borrow<Q>,
    {
        todo!()
    }
}
