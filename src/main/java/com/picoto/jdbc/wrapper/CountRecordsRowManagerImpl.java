package com.picoto.jdbc.wrapper;

import java.util.ArrayList;
import java.util.List;

public class CountRecordsRowManagerImpl implements RowManagerLambda<Integer> {

	@Override
	public List<Integer> mapRow(Cursor c) {
		List<Integer> lista = new ArrayList<Integer>(1);
		if (c.next()) {
			lista.add(c.getInt(1));
		}
		return lista;
	}

}
