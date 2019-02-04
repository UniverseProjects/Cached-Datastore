package com.universeprojects.cacheddatastore.querybuilder;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@SuppressWarnings({"unused", "UnusedReturnValue"})
public class QueryBuilder {

    protected final Query query;
    protected final List<Query.Filter> filters = new ArrayList<>();
    protected Query.CompositeFilterOperator compositeFilterOperator = Query.CompositeFilterOperator.AND;
    protected FetchOptions fetchOptions = FetchOptions.Builder.withDefaults();

    public QueryBuilder(Query query) {
        this.query = query;
    }

    public interface MainBuilder {
        FilterBuilder filterOn(String fieldName);
        MainBuilder filterAnd();
        MainBuilder filterOr();
        MainBuilder sortOn(String fieldName, Query.SortDirection direction);
        MainBuilder sortOnAsc(String fieldName);
        MainBuilder sortOnDesc(String fieldName);
        MainBuilder withCursor(Cursor cursor);
        MainBuilder withCursor(String cursorString);
        MainBuilder withOffset(int offset);
        MainBuilder withLimit(int limit);
        MainBuilder withPrefetchSize(int limit);
        MainBuilder withChunkSize(int limit);
        Query buildQuery();
        Query buildKeyOnlyQuery();
        FetchOptions buildFetchOptions();
    }

    public interface FilterBuilder {
        <T> MainBuilder isEquals(T value);
        <T> MainBuilder isNotEquals(T value);
        <T> MainBuilder isGreaterThan(T value);
        <T> MainBuilder isLessThan(T value);
        <T> MainBuilder isGreaterThanOrEquals(T value);
        <T> MainBuilder isLessThanOrEquals(T value);
        <T> MainBuilder in(Collection<T> value);
        <T> MainBuilder filter(Query.FilterOperator operator, T value);
    }

    protected <V, S> S serializeFilterValue(V value) {
        //noinspection unchecked
        return (S) value;
    }

    public class Builder implements MainBuilder, FilterBuilder {
        private String currentFilterProperty;

        @Override
        public FilterBuilder filterOn(String fieldName) {
            currentFilterProperty = fieldName;
            return this;
        }

        @Override
        public MainBuilder filterAnd() {
            compositeFilterOperator = Query.CompositeFilterOperator.AND;
            return this;
        }

        @Override
        public MainBuilder filterOr() {
            compositeFilterOperator = Query.CompositeFilterOperator.OR;
            return this;
        }

        @Override
        public MainBuilder sortOn(String fieldName, Query.SortDirection direction) {
            query.addSort(fieldName, direction);
            return this;
        }

        @Override
        public MainBuilder sortOnAsc(String fieldName) {
            return sortOn(fieldName, Query.SortDirection.ASCENDING);
        }

        @Override
        public MainBuilder sortOnDesc(String fieldName) {
            return sortOn(fieldName, Query.SortDirection.DESCENDING);
        }

        @Override
        public MainBuilder withCursor(Cursor cursor) {
            fetchOptions.startCursor(cursor);
            return this;
        }

        @Override
        public MainBuilder withCursor(String cursorString) {
            fetchOptions.startCursor(Cursor.fromWebSafeString(cursorString));
            return this;
        }

        @Override
        public MainBuilder withOffset(int offset) {
            fetchOptions.offset(offset);
            return this;
        }

        @Override
        public MainBuilder withLimit(int limit) {
            fetchOptions.limit(limit);
            return this;
        }

        @Override
        public MainBuilder withPrefetchSize(int prefetchSize) {
            fetchOptions.prefetchSize(prefetchSize);
            return this;
        }

        @Override
        public MainBuilder withChunkSize(int chunkSize) {
            fetchOptions.chunkSize(chunkSize);
            return this;
        }

        @Override
        public Query buildQuery() {
            if(filters.size() == 1) {
                query.setFilter(filters.get(0));
            } else if(filters.size() > 1) {
                query.setFilter(new Query.CompositeFilter(compositeFilterOperator, filters));
            }
            return query;
        }

        @Override
        public Query buildKeyOnlyQuery() {
            buildQuery();
            query.setKeysOnly();
            return query;
        }

        @Override
        public FetchOptions buildFetchOptions() {
            return fetchOptions;
        }

        @Override
        public MainBuilder filter(Query.FilterOperator filterOperator, Object value) {
            filters.add(new Query.FilterPredicate(currentFilterProperty, filterOperator, serializeFilterValue(value)));
            currentFilterProperty = null;
            return this;
        }

        @Override
        public <T> MainBuilder isEquals(T value) {
            return filter(Query.FilterOperator.EQUAL, value);
        }

        @Override
        public <T> MainBuilder isNotEquals(T value) {
            return filter(Query.FilterOperator.NOT_EQUAL, value);
        }

        @Override
        public <T> MainBuilder isGreaterThan(T value) {
            return filter(Query.FilterOperator.GREATER_THAN, value);
        }

        @Override
        public <T> MainBuilder isLessThan(T value) {
            return filter(Query.FilterOperator.LESS_THAN, value);
        }

        @Override
        public <T> MainBuilder isGreaterThanOrEquals(T value) {
            return filter(Query.FilterOperator.GREATER_THAN_OR_EQUAL, value);
        }

        @Override
        public <T> MainBuilder isLessThanOrEquals(T value) {
            return filter(Query.FilterOperator.LESS_THAN_OR_EQUAL, value);
        }

        @Override
        public <T> MainBuilder in(Collection<T> value) {
            return filter(Query.FilterOperator.IN, value);
        }
    }
}
