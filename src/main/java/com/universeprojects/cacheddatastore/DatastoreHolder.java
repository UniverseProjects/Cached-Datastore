package com.universeprojects.cacheddatastore;

import com.google.appengine.api.memcache.MemcacheService;

public class DatastoreHolder {
    public static CachedDatastoreService getDatastore() {
        return DatastoreHolder.datastore.get();
    }

    public static MemcacheService getMemCache() {
        return getDatastore().getMC();
    }

    public static void reset() {
        DatastoreHolder.datastore.remove();
    }

    private static ThreadLocal<CachedDatastoreService> datastore = new ThreadLocal<CachedDatastoreService>() {
        @Override
        protected CachedDatastoreService initialValue() {
            return new CachedDatastoreService();
        }
    };
}
