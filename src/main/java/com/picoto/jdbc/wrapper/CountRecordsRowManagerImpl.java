package com.picoto.jdbc.wrapper;

public class CountRecordsRowManagerImpl implements RowManagerLambda<Integer> {

	@Override
	public Integer mapRow(Cursor c) {
		return c.getInt(1);
	}

}
