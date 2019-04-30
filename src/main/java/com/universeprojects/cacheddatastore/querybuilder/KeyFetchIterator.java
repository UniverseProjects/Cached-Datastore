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
    //TODO make this work properly with cursor etc.
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
        final Entity entity = rawIterator.next();
        if(entity == null) {
            return null;
        }
        final CachedEntity cachedEntity = cds().getIfExists(entity.getKey());
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
