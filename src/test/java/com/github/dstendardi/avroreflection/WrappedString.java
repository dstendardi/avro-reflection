package com.github.dstendardi.avroreflection;

public class WrappedString<T> {

    private final T id;

    public WrappedString(T id) {
        this.id = id;
    }

    public T getId() {
        return id;
    }
}
