package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A collection class for TrackedSpannerTable objects that provides both standard
 * Collection functionality and specialized table lookup methods.
 */
public class TrackedSpannerTableCollection implements Collection<TrackedSpannerTable>, Serializable {

    private final List<TrackedSpannerTable> tables = new ArrayList<>();

    /**
     * Find a table by its fully qualified name (catalog.name)
     * 
     * @param fullyQualifiedName The fully qualified table name
     * @return The table if found, or empty if no matching table exists
     */
    public Optional<TrackedSpannerTable> getTableByFullyQualifiedName(String fullyQualifiedName) {
        return tables.stream()
                .filter(table -> table.getFullyQualifiedTableName().equals(fullyQualifiedName))
                .findFirst();
    }

    // Collection interface methods
    @Override
    public int size() {
        return tables.size();
    }

    @Override
    public boolean isEmpty() {
        return tables.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return tables.contains(o);
    }

    @Override
    public Iterator<TrackedSpannerTable> iterator() {
        return tables.iterator();
    }

    @Override
    public Object[] toArray() {
        return tables.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return tables.toArray(a);
    }

    @Override
    public boolean add(TrackedSpannerTable table) {
        if (table == null) {
            return false;
        }
        
        return tables.add(table);
    }

    @Override
    public boolean remove(Object o) {
        if (!(o instanceof TrackedSpannerTable)) {
            return false;
        }
        
        int index = tables.indexOf(o);
        if (index >= 0) {
          tables.remove(index);
          return true;
        }

        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return tables.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends TrackedSpannerTable> c) {
        boolean changed = false;
        for (TrackedSpannerTable table : c) {
            changed |= add(table);
        }
        return changed;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean changed = false;
        for (Object o : c) {
            changed |= remove(o);
        }
        return changed;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        List<TrackedSpannerTable> tablesToRemove = tables.stream()
                .filter(Predicate.not(c::contains))
                .collect(Collectors.toList());
        
        boolean changed = false;
        for (TrackedSpannerTable table : tablesToRemove) {
            changed |= remove(table);
        }
        return changed;
    }

    @Override
    public void clear() {
        tables.clear();
    }
    
    @Override
    public String toString() {
        return "TrackedSpannerTableCollection{size=" + size() + ", tables=" + tables + "}";
    }
}