package com.universeprojects.cacheddatastore.querybuilder;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.universeprojects.cacheddatastore.CachedEntity;

public class CachedEntityQueryBuilder extends GenericQueryBuilder<CachedEntity> {

    private CachedEntityQueryBuilder(Query query) {
        super(query);
    }

    public static GenericMainBuilder<CachedEntity> forKind(String kind) {
        final Query query = new Query(kind);
        return new CachedEntityQueryBuilder(query).builder;
    }

    public static GenericMainBuilder<CachedEntity> forAncestor(Key ancestor) {
        final Query query = new Query(ancestor);
        return new CachedEntityQueryBuilder(query).builder;
    }

    public static GenericMainBuilder<CachedEntity> forAncestorAndKind(String kind, Key ancestor) {
        final Query query = new Query(kind, ancestor);
        return new CachedEntityQueryBuilder(query).builder;
    }

    @Override
    protected CachedEntity transform(CachedEntity cachedEntity) {
        return cachedEntity;
    }
}
