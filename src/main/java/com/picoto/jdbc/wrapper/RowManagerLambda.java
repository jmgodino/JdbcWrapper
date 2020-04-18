package com.picoto.jdbc.wrapper;

@FunctionalInterface
public interface RowManagerLambda<T> {

	T mapRow(Cursor c);
}
