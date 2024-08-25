package com.universeprojects.cacheddatastore;

import java.util.ArrayList;
import java.util.List;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.memcache.Expiration;

public class CachedQueryService {

	private CachedDatastoreService ds;
	private QueryHelper query;
	
	private static String CACHED_QUERY_MC_KEY = "CACHED_QUERY-";
	private static String CACHED_QUERY_COMPONENT_SEPARATOR = "---";
	
	public CachedQueryService(CachedDatastoreService ds, QueryHelper query) {
		this.ds = ds;
		this.query = query;
	}
	
	private String cachedQueryKey(Object... components) {
	    List<String> list = new ArrayList<String>();
	    for (Object component : components) {
	        list.add(component != null ? component.toString() : "null");
	    }

	    StringBuilder joinedString = new StringBuilder();
	    for (int i = 0; i < list.size(); i++) {
	        joinedString.append(list.get(i));
	        if (i < list.size()) {
	            joinedString.append(CACHED_QUERY_COMPONENT_SEPARATOR);
	        }
	    }

	    return CACHED_QUERY_MC_KEY + joinedString.toString();
	}
	
	private List<Key> getCachedQuery(String cachedQueryKey) {
		@SuppressWarnings("unchecked")
		List<Key> cachedKeys = (List<Key>) ds.getMC().get(cachedQueryKey);
		
		return cachedKeys;
	}
	
	private void saveCachedQuery(String cachedQueryKey, List<Key> keys, Expiration expiration) {
		ds.getMC().put(cachedQueryKey, keys, expiration);
	}
	
	
	
	
	
	public List<CachedEntity> getCachedFilteredList(String kind, int limit, Expiration cacheExpiry) {
		String queryKey = cachedQueryKey(kind, limit + "");
		List<Key> keys = getCachedQuery(queryKey);
		if (keys == null) {
			keys = query.getFilteredList_Keys(kind, limit);
			saveCachedQuery(queryKey, keys, cacheExpiry);
		}
		
		return ds.get(keys);
	}
	
	
	public List<CachedEntity> getCachedFilteredList(String kind, String fieldName, Object equalToValue, Expiration cacheExpiry) {
		String queryKey = cachedQueryKey(kind, fieldName, equalToValue);
		List<Key> keys = getCachedQuery(queryKey);
		if (keys == null) {
			keys = query.getFilteredList_Keys(kind, fieldName, equalToValue);
			saveCachedQuery(queryKey, keys, cacheExpiry);
		}
		
		return ds.get(keys);
	}
	
	
	public List<CachedEntity> getCachedFilteredList(String kind, String fieldName, Object equalToValue, String fieldName2, Object equalToValue2, Expiration cacheExpiry) {
		String queryKey = cachedQueryKey(kind, fieldName, equalToValue);
		List<Key> keys = getCachedQuery(queryKey);
		if (keys == null) {
			keys = query.getFilteredList_Keys(kind, fieldName, equalToValue, fieldName2, equalToValue2);
			saveCachedQuery(queryKey, keys, cacheExpiry);
		}
		
		return ds.get(keys);
	}
	
	

}
