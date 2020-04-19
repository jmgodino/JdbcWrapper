package com.picoto.jdbc.wrapper;

import java.sql.Connection;

public class JdbcWrapperFactory {
	
	public static <T> JdbcWrapper<T> getJdbcWrapper(Class<T> clase) {
		return (JdbcWrapper<T>)new JdbcWrapperImpl<T>();
	}
	
	public static <T> JdbcWrapper<T> getJdbcWrapper(Class<T> clase, Connection con, boolean autoCommit, boolean autoClose) {
		return (JdbcWrapper<T>)new JdbcWrapperImpl<T>(con, autoCommit, autoClose);
	}
	
	public static  <T> JdbcNamedWrapper<T> getJdbcNamedWrapper(Class<T> clase, Connection con, boolean autoCommit, boolean autoClose) {
		return (JdbcNamedWrapper<T>)new JdbcNamedWrapperImpl<T>(con, autoCommit, autoClose);
	}

}
