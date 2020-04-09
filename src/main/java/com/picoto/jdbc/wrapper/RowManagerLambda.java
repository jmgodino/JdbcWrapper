package com.picoto.jdbc.wrapper;

import java.util.List;

@FunctionalInterface
public interface RowManagerLambda<T> {

	List<T> mapRow(Cursor c);
}
