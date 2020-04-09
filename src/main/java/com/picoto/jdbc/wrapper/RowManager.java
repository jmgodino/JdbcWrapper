package com.picoto.jdbc.wrapper;

import java.sql.ResultSet;
import java.util.List;

public interface RowManager<T> {

	List<T> mapRow(ResultSet rs);

}
