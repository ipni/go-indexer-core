# Delegate Store

**This is a placeholder for a delegated store**

The delegate store gets stored values from other indexers instead of from a value store.  An indexer can serve as a caching-only delegating indexer in an indexer hierarchy.  Such an indexer is created by giving it a delegate store as its value store.

A delegate store does not store values, but may maintain a temporarily negative cache.

A delegate store must filter duplicate responses coming from multiple delegate indexers, since a provider may populate multiple indexers with the same index data.
