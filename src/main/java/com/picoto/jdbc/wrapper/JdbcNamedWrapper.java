package com.picoto.jdbc.wrapper;

import java.sql.Connection;
import java.util.List;

public interface JdbcNamedWrapper<T> {

	void setConnection(Connection con);

	void setAutoCommit(boolean autoCommit);

	void setAutoClose(boolean autoClose);

	List<T> query(String queryStr, NamedParameterManager paramManager, RowManagerLambda<T> rowManager);

	List<T> query(String queryStr, RowManagerLambda<T> rowManager);

	int update(String updateStr, NamedParameterManager paramManager);
	
	void delete(String updateStr, NamedParameterManager paramManager);

	void insert(String updateStr, NamedParameterManager paramManager) ;

	int count(String queryCountStr);

	JdbcNamedQuery<T> getQuery(String queryStr);

	T getRecord(String queryStr, NamedParameterManager paramManager, RowManagerLambda<T> rowManager);
	
	void debug(String message);

}
