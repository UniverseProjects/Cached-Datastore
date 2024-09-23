package com.universeprojects.cacheddatastore;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.memcache.ErrorHandler;
import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.Stats;

@SuppressWarnings("deprecation")
public class CachedMemcacheService implements MemcacheService {

    private final MemcacheService memcacheService;

    public CachedMemcacheService(MemcacheService memcacheService) {
        this.memcacheService = memcacheService;
    }

    private static class CachedIdentifiableValue implements IdentifiableValue {
        private final IdentifiableValue wrappedValue;

        public CachedIdentifiableValue(IdentifiableValue wrappedValue) {
            this.wrappedValue = wrappedValue;
        }

        @Override
        public Object getValue() {
            return unwrap(wrappedValue.getValue());
        }

    }

    private static class CachedObject implements Serializable {
        private final Object value;
        private final Date expiryDate;

        public CachedObject(Object value, Expiration expiration) {
            this.value = value;
            this.expiryDate = expiration != null ? 
                new Date(System.currentTimeMillis() + expiration.getMillisecondsValue()) : null;
        }

        public Object getValue() {
            return value;
        }

        public boolean isExpired() {
            return expiryDate != null && System.currentTimeMillis() > expiryDate.getTime();
        }
    }

    private CachedObject wrap(Object value, Expiration expiration) {
        return new CachedObject(value, expiration);
    }

    private static Object unwrap(Object cachedObject) {
        if (cachedObject instanceof CachedObject) {
            CachedObject co = (CachedObject) cachedObject;
            return co.isExpired() ? null : co.getValue();
        }
        return cachedObject;
    }

    @Override
    public void setNamespace(String namespace) {
        memcacheService.setNamespace(namespace);
    }

    @Override
    public Object get(Object key) {
        return unwrap(memcacheService.get(key));
    }

    @Override
    public IdentifiableValue getIdentifiable(Object key) {
        IdentifiableValue iv = memcacheService.getIdentifiable(key);
        return iv != null ? new CachedIdentifiableValue(iv) : null;
    }

    @Override
    public <T> Map<T, IdentifiableValue> getIdentifiables(Collection<T> keys) {
        Map<T, IdentifiableValue> result = memcacheService.getIdentifiables(keys);
        for (Map.Entry<T, IdentifiableValue> entry : result.entrySet()) {
            entry.setValue(new CachedIdentifiableValue(entry.getValue()));
        }
        return result;
    }

    @Override
    public boolean contains(Object key) {
        Object value = memcacheService.get(key);
        return value != null && unwrap(value) != null;
    }

    @Override
    public <T> Map<T, Object> getAll(Collection<T> keys) {
        Map<T, Object> result = memcacheService.getAll(keys);
        for (Map.Entry<T, Object> entry : result.entrySet()) {
            entry.setValue(unwrap(entry.getValue()));
        }
        return result;
    }

    @Override
    public boolean put(Object key, Object value, Expiration expiration, SetPolicy policy) {
        return memcacheService.put(key, wrap(value, expiration), null, policy);
    }

    @Override
    public void put(Object key, Object value, Expiration expiration) {
        memcacheService.put(key, wrap(value, expiration));
    }

    @Override
    public void put(Object key, Object value) {
        memcacheService.put(key, wrap(value, null));
    }

    @Override
    public <T> Set<T> putAll(Map<T, ?> values, Expiration expiration, SetPolicy policy) {
        Map<T, Object> wrappedValues = new HashMap<>();
        for (Map.Entry<T, ?> entry : values.entrySet()) {
            wrappedValues.put(entry.getKey(), wrap(entry.getValue(), expiration));
        }
        return memcacheService.putAll(wrappedValues, null, policy);
    }

    @Override
    public void putAll(Map<?, ?> values, Expiration expiration) {
        Map<Object, Object> wrappedValues = new HashMap<>();
        for (Map.Entry<?, ?> entry : values.entrySet()) {
            wrappedValues.put(entry.getKey(), wrap(entry.getValue(), expiration));
        }
        memcacheService.putAll(wrappedValues);
    }

    @Override
    public void putAll(Map<?, ?> values) {
        putAll(values, null);
    }

    @Override
    public boolean putIfUntouched(Object key, IdentifiableValue oldValue, Object newValue, Expiration expiration) {
        return memcacheService.putIfUntouched(key, oldValue, wrap(newValue, expiration));
    }

    @Override
    public boolean putIfUntouched(Object key, IdentifiableValue oldValue, Object newValue) {
        return putIfUntouched(key, oldValue, newValue, null);
    }

    @Override
    public <T> Set<T> putIfUntouched(Map<T, CasValues> values) {
        return putIfUntouched(values, null);
    }

    @Override
    public <T> Set<T> putIfUntouched(Map<T, CasValues> values, Expiration expiration) {
        Map<T, CasValues> wrappedValues = new HashMap<>();
        for (Map.Entry<T, CasValues> entry : values.entrySet()) {
            CasValues casValues = entry.getValue();
            wrappedValues.put(entry.getKey(), new CasValues(casValues.getOldValue(), wrap(casValues.getNewValue(), expiration)));
        }
        return memcacheService.putIfUntouched(wrappedValues);
    }

    @Override
    public boolean delete(Object key) {
        return memcacheService.delete(key);
    }

    @Override
    public boolean delete(Object key, long millisNoReAdd) {
        return memcacheService.delete(key, millisNoReAdd);
    }

    @Override
    public <T> Set<T> deleteAll(Collection<T> keys) {
        return memcacheService.deleteAll(keys);
    }

    @Override
    public <T> Set<T> deleteAll(Collection<T> keys, long millisNoReAdd) {
        return memcacheService.deleteAll(keys, millisNoReAdd);
    }

    @Override
    public Long increment(Object key, long delta) {
        return memcacheService.increment(key, delta);
    }

    @Override
    public Long increment(Object key, long delta, Long initialValue) {
        return memcacheService.increment(key, delta, initialValue);
    }

    @Override
    public <T> Map<T, Long> incrementAll(Collection<T> keys, long delta) {
        return memcacheService.incrementAll(keys, delta);
    }

    @Override
    public <T> Map<T, Long> incrementAll(Collection<T> keys, long delta, Long initialValue) {
        return memcacheService.incrementAll(keys, delta, initialValue);
    }

    @Override
    public <T> Map<T, Long> incrementAll(Map<T, Long> offsets) {
        return memcacheService.incrementAll(offsets);
    }

    @Override
    public <T> Map<T, Long> incrementAll(Map<T, Long> offsets, Long initialValue) {
        return memcacheService.incrementAll(offsets, initialValue);
    }

    @Override
    public void clearAll() {
        memcacheService.clearAll();
    }

    @Override
    public Stats getStatistics() {
        return memcacheService.getStatistics();
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return memcacheService.getErrorHandler();
    }

    @Override
    public String getNamespace() {
        return memcacheService.getNamespace();
    }

    @Override
    public void setErrorHandler(ErrorHandler arg0) {
        memcacheService.setErrorHandler(arg0);
    }
}
