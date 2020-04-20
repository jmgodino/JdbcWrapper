package com.picoto.jdbc.wrapper;

import java.sql.Connection;
import java.util.List;

public interface JdbcWrapper<T> {

	void setConnection(Connection con);

	void setAutoCommit(boolean autoCommit);

	void setAutoClose(boolean autoClose);

	List<T> query(String queryStr, ParameterManager paramManager, RowManagerLambda<T> rowManager);

	List<T> query(String queryStr, RowManagerLambda<T> rowManager);

	int update(String updateStr, ParameterManager paramManager);
	
	void delete(String updateStr, ParameterManager paramManager);

	void insert(String updateStr, ParameterManager paramManager) ;

	int count(String queryCountStr);

	int count(String queryCountStr, ParameterManager paramManager);
	
	void callFunction(String procedureStr, ProcedureParameterManager paramManager,
			ProcedureOutputParameterManager resultManager);

	void callProcedure(String procedureStr, ProcedureParameterManager paramManager,
			ProcedureOutputParameterManager resultManager, boolean isFunction);

	JdbcQuery<T> getQuery(String queryStr);

	T getObject(String queryStr, ParameterManager paramManager, RowManagerLambda<T> rowManager);
	
	void debug(String message);

}
