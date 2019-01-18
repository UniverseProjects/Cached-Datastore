package com.universeprojects.cacheddatastore.querybuilder;

import com.google.appengine.api.datastore.*;
import com.universeprojects.cacheddatastore.CachedDatastoreService;
import com.universeprojects.cacheddatastore.CachedEntity;
import com.universeprojects.cacheddatastore.DatastoreHolder;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public abstract class KeyFetchIterator<T> implements QueryResultIterator<T> {

    private final QueryResultIterator<Entity> rawIterator;
    private final int fetchChunksize;
    private final Deque<CachedEntity> fetchedEntities = new LinkedList<>();

    public KeyFetchIterator(QueryResultIterator<Entity> rawIterator, int fetchChunkSize) {
        this.rawIterator = rawIterator;
        this.fetchChunksize = fetchChunkSize;
    }

    @Override
    public boolean hasNext() {
        return rawIterator.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            return null;
        }
        if (fetchedEntities.isEmpty()) {
            List<Key> keysToFetch = new ArrayList<>();
            for (int i = 0; i < fetchChunksize; i++) {
                if (!rawIterator.hasNext()) {
                    break;
                }
                final Entity next = rawIterator.next();
                keysToFetch.add(next.getKey());
            }
            fetchedEntities.addAll(cds().get(keysToFetch));
        }
        final CachedEntity cachedEntity = fetchedEntities.removeFirst();
        return transform(cachedEntity);
    }

    @Override
    public List<Index> getIndexList() {
        return rawIterator.getIndexList();
    }

    @Override
    public Cursor getCursor() {
        return rawIterator.getCursor();
    }

    private CachedDatastoreService cds() {
        return DatastoreHolder.getDatastore();
    }

    private DatastoreService ds() {
        return DatastoreServiceFactory.getDatastoreService();
    }

    protected abstract T transform(CachedEntity entity);
}
