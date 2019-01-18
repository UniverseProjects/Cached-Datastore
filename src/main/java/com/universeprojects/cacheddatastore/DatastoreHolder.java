package com.universeprojects.cacheddatastore;

import com.google.appengine.api.memcache.MemcacheService;
import com.universeprojects.cacheddatastore.CachedDatastoreService;

public class DatastoreHolder {
    public static CachedDatastoreService getDatastore() {
        return datastore.get();
    }

    public static MemcacheService getMemCache() {
        return getDatastore().getMC();
    }

    private static ThreadLocal<CachedDatastoreService> datastore = new ThreadLocal<CachedDatastoreService>() {
        @Override
        protected CachedDatastoreService initialValue() {
            return new CachedDatastoreService();
        }
    };
}
