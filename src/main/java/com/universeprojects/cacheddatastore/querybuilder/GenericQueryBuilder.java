package com.universeprojects.cacheddatastore.querybuilder;

import com.google.appengine.api.datastore.*;
import com.universeprojects.cacheddatastore.CachedDatastoreService;
import com.universeprojects.cacheddatastore.CachedEntity;
import com.universeprojects.cacheddatastore.DatastoreHolder;

import java.util.*;

@SuppressWarnings({"unused", "UnusedReturnValue"})
public abstract class GenericQueryBuilder<R> extends QueryBuilder {

    public static final int DEFAULT_FETCH_CHUNK_SIZE = 500;
    protected final GenericBuilder builder = new GenericBuilder();

    protected GenericQueryBuilder(Query query) {
        super(query);
    }

    public interface GenericMainBuilder<B> extends MainBuilder {
        Iterable<B> asIterable();
        Iterator<B> asIterator();
        QueryResultIterable<B> asQueryResultIterable();
        QueryResultIterator<B> asQueryResultIterator();
        Iterable<B> asIterable(int fetchChunkSize);
        Iterator<B> asIterator(int fetchChunkSize);
        QueryResultIterable<B> asQueryResultIterable(int fetchChunkSize);
        QueryResultIterator<B> asQueryResultIterator(int fetchChunkSize);
        Set<B> asSet();
        List<B> asList();
        B asSingleEntity();
        Iterable<Key> asKeyIterable();
        Iterator<Key> asKeyIterator();
        Set<Key> asKeySet();
        List<Key> asKeyList();
        int count();

        @Override
        GenericFilterBuilder<B> filterOn(String fieldName);

        @Override
        GenericMainBuilder<B> filterAnd();

        @Override
        GenericMainBuilder<B> filterOr();

        @Override
        GenericMainBuilder<B> sortOn(String fieldName, Query.SortDirection direction);

        @Override
        GenericMainBuilder<B> sortOnAsc(String fieldName);

        @Override
        GenericMainBuilder<B> sortOnDesc(String fieldName);

        @Override
        GenericMainBuilder<B> withCursor(Cursor cursor);

        @Override
        GenericMainBuilder<B> withCursor(String cursorString);

        @Override
        GenericMainBuilder<B> withOffset(int offset);

        @Override
        GenericMainBuilder<B> withLimit(int limit);

        @Override
        GenericMainBuilder<B> withPrefetchSize(int limit);

        @Override
        GenericMainBuilder<B> withChunkSize(int limit);
    }

    public interface GenericFilterBuilder<B> extends FilterBuilder {
        @Override
        <T> GenericMainBuilder<B> isEquals(T value);

        @Override
        <T> GenericMainBuilder<B> isNotEquals(T value);

        @Override
        <T> GenericMainBuilder<B> isGreaterThan(T value);

        @Override
        <T> GenericMainBuilder<B> isLessThan(T value);

        @Override
        <T> GenericMainBuilder<B> isGreaterThanOrEquals(T value);

        @Override
        <T> GenericMainBuilder<B> isLessThanOrEquals(T value);

        @Override
        <T> GenericMainBuilder<B> in(Collection<T> value);

        @Override
        <T> GenericMainBuilder<B> filter(Query.FilterOperator operator, T value);
    }

    protected abstract R transform(CachedEntity cachedEntity);

    public class GenericBuilder extends Builder implements GenericMainBuilder<R>, GenericFilterBuilder<R> {

        private CachedDatastoreService cds() {
            return DatastoreHolder.getDatastore();
        }

        private DatastoreService ds() {
            return DatastoreServiceFactory.getDatastoreService();
        }

        private KeyFetchIterator<R> generateIterator() {
            return generateIterator(DEFAULT_FETCH_CHUNK_SIZE);
        }

        private KeyFetchIterator<R> generateIterator(int fetchChunkSize) {
            if(fetchOptions.getChunkSize() == null) {
                fetchOptions.chunkSize(fetchChunkSize);
            }
            final Query query = buildQuery();
            query.setKeysOnly();
            final QueryResultIterator<Entity> iterator = ds().prepare(query).asQueryResultIterator(buildFetchOptions());
            return new KeyFetchIterator<R>(iterator, fetchChunkSize) {
                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove");
                }

                @Override
                protected R transform(CachedEntity entity) {
                    return GenericQueryBuilder.this.transform(entity);
                }
            };
        }

        @Override
        public Iterable<R> asIterable() {
            return asQueryResultIterable();
        }

        @Override
        public Iterator<R> asIterator() {
            return asQueryResultIterator();
        }

        @Override
        public QueryResultIterable<R> asQueryResultIterable() {
            return new QueryResultIterable<R>() {
                @Override
                public QueryResultIterator<R> iterator() {
                    return generateIterator();
                }
            };
        }

        @Override
        public QueryResultIterator<R> asQueryResultIterator() {
            return generateIterator();
        }

        @Override
        public Iterable<R> asIterable(int fetchChunkSize) {
            return asQueryResultIterable(fetchChunkSize);
        }

        @Override
        public Iterator<R> asIterator(int fetchChunkSize) {
            return asQueryResultIterator(fetchChunkSize);
        }

        @Override
        public QueryResultIterable<R> asQueryResultIterable(final int fetchChunkSize) {
            return new QueryResultIterable<R>() {
                @Override
                public QueryResultIterator<R> iterator() {
                    return generateIterator(fetchChunkSize);
                }
            };
        }

        @Override
        public QueryResultIterator<R> asQueryResultIterator(int fetchChunkSize) {
            return generateIterator(fetchChunkSize);
        }

        @Override
        public Set<R> asSet() {
            return asCollection(new LinkedHashSet<R>());
        }

        @Override
        public List<R> asList() {
            return asCollection(new ArrayList<R>());
        }

        private <T extends Collection<R>> T asCollection(T coll) {
            final List<Key> keyList = asKeyList();
            final List<CachedEntity> cachedEntityList = cds().get(keyList);
            for(CachedEntity cachedEntity : cachedEntityList) {
                coll.add(transform(cachedEntity));
            }
            return coll;
        }

        @Override
        public R asSingleEntity() {
            final Query query = buildKeyOnlyQuery();
            final CachedEntity cachedEntity = cds().fetchSingleEntity(query);
            return transform(cachedEntity);
        }

        @Override
        public Iterable<Key> asKeyIterable() {
            return new Iterable<Key>() {
                @Override
                public Iterator<Key> iterator() {
                    return asKeyIterator();
                }
            };
        }

        @Override
        public Iterator<Key> asKeyIterator() {
            final Iterator<Entity> iterator = ds().prepare(buildKeyOnlyQuery()).asIterator(buildFetchOptions());
            return new Iterator<Key>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Key next() {
                    Entity entity = iterator.next();
                    if(entity == null) {
                        return null;
                    } else {
                        return entity.getKey();
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove");
                }
            };
        }

        @Override
        public Set<Key> asKeySet() {
            return asKeyCollection(new LinkedHashSet<Key>());
        }

        @Override
        public List<Key> asKeyList() {
            return asKeyCollection(new ArrayList<Key>());
        }

        private <T extends Collection<Key>> T asKeyCollection(T coll) {
            final Query query = buildKeyOnlyQuery();
            final PreparedQuery preparedQuery = ds().prepare(query);
            final List<Entity> list = preparedQuery.asList(buildFetchOptions());
            for(Entity entity : list) {
                coll.add(entity.getKey());
            }
            return coll;
        }

        @Override
        public int count() {
            final Query query = buildQuery();
            return ds().prepare(query).countEntities(buildFetchOptions());
        }

        @Override
        public GenericBuilder filterOn(String fieldName) {
            super.filterOn(fieldName);
            return this;
        }

        @Override
        public GenericBuilder filterAnd() {
            super.filterAnd();
            return this;
        }

        @Override
        public GenericBuilder filterOr() {
            super.filterOr();
            return this;
        }

        @Override
        public GenericBuilder sortOn(String fieldName, Query.SortDirection direction) {
            super.sortOn(fieldName, direction);
            return this;
        }

        @Override
        public GenericBuilder sortOnAsc(String fieldName) {
            super.sortOnAsc(fieldName);
            return this;
        }

        @Override
        public GenericBuilder sortOnDesc(String fieldName) {
            super.sortOnDesc(fieldName);
            return this;
        }

        @Override
        public GenericBuilder withCursor(Cursor cursor) {
            super.withCursor(cursor);
            return this;
        }

        @Override
        public GenericBuilder withCursor(String cursorString) {
            super.withCursor(cursorString);
            return this;
        }

        @Override
        public GenericBuilder withOffset(int offset) {
            super.withOffset(offset);
            return this;
        }

        @Override
        public GenericBuilder withLimit(int limit) {
            super.withLimit(limit);
            return this;
        }

        @Override
        public GenericBuilder withPrefetchSize(int prefetchSize) {
            super.withPrefetchSize(prefetchSize);
            return this;
        }

        @Override
        public GenericBuilder withChunkSize(int chunkSize) {
            super.withChunkSize(chunkSize);
            return this;
        }

        @Override
        public Query buildQuery() {
            return super.buildQuery();
        }

        @Override
        public Query buildKeyOnlyQuery() {
            return super.buildKeyOnlyQuery();
        }

        @Override
        public FetchOptions buildFetchOptions() {
            return super.buildFetchOptions();
        }

        @Override
        public GenericBuilder filter(Query.FilterOperator filterOperator, Object value) {
            super.filter(filterOperator, value);
            return this;
        }

        @Override
        public <T> GenericBuilder isEquals(T value) {
            super.isEquals(value);
            return this;
        }

        @Override
        public <T> GenericBuilder isNotEquals(T value) {
            super.isNotEquals(value);
            return this;
        }

        @Override
        public <T> GenericBuilder isGreaterThan(T value) {
            super.isGreaterThan(value);
            return this;
        }

        @Override
        public <T> GenericBuilder isLessThan(T value) {
            super.isLessThan(value);
            return this;
        }

        @Override
        public <T> GenericBuilder isGreaterThanOrEquals(T value) {
            super.isGreaterThanOrEquals(value);
            return this;
        }

        @Override
        public <T> GenericBuilder isLessThanOrEquals(T value) {
            super.isLessThanOrEquals(value);
            return this;
        }

        @Override
        public <T> GenericBuilder in(Collection<T> value) {
            super.in(value);
            return this;
        }
    }
}
