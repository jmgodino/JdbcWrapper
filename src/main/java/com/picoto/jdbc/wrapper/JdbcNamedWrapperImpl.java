package com.picoto.jdbc.wrapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

public class JdbcNamedWrapperImpl<T> extends JdbcBase implements JdbcNamedWrapper<T> {

	private Connection con;
	private boolean autoCommit = false;
	private boolean autoClose = false;

	public JdbcNamedWrapperImpl() {
		super();
		// TODO Auto-generated constructor stub
	}

	public JdbcNamedWrapperImpl(Connection con) {
		super();
		this.con = con;
		this.autoCommit = false;
		this.autoClose = false;
	}

	public JdbcNamedWrapperImpl(Connection con, boolean autoCommit) {
		super();
		this.con = con;
		this.autoCommit = autoCommit;
		this.autoClose = false;
	}

	public JdbcNamedWrapperImpl(Connection con, boolean autoCommit, boolean autoClose) {
		super();
		this.con = con;
		this.autoCommit = autoCommit;
		this.autoClose = autoClose;
	}

	protected Connection getConection() {
		return con;
	}

	public void setConnection(Connection con) {
		this.con = con;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	public boolean isAutoClose() {
		return autoClose;
	}

	public void setAutoClose(boolean autoClose) {
		this.autoClose = autoClose;
	}

	@Override
	public List<T> query(String namedQueryStr, NamedParameterManager paramManager, RowManagerLambda<T> rowManager) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. No hay conexión a BB.DD.");
			}

			if (namedQueryStr == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. No se ha definido la consulta");
			}

			if (paramManager == null) {
				throw new JdbcWrapperException(
						"Error ejecutando consulta. Es necesario definir un gestor de parámetros");
			}

			if (rowManager == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. Es necesario definir un gestor de filas");
			}

			ClassWrapper<String> wrap = new ClassWrapper<>();
			wrap.setValue(namedQueryStr);
			List<String> fields = JdbcUtils.extractFields(wrap);
			ps = getPreparedStatement(con, wrap.getValue());
			NamedParameterSetter setter = new NamedParameterSetter();
			setter.setStatement(ps);
			setter.setFields(fields);
			paramManager.configureParameters(setter);
			rs = ps.executeQuery();
			Cursor cursor = new Cursor();
			cursor.setResultSet(rs);

			List<T> lista = new LinkedList<>();
			while (rs.next()) {
				lista.add(rowManager.mapRow(cursor));
			}
			return lista;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando named query", e);
		} finally {
			close(rs);
			close(ps);
			if (isAutoClose()) {
				close(con);
			}
		}
	}

	@Override
	public List<T> query(String queryStr, RowManagerLambda<T> rowManager) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. No hay conexión a BB.DD.");
			}

			if (queryStr == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. No se ha definido la consulta");
			}

			if (rowManager == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. Es necesario definir un gestor de filas");
			}

			if (queryStr.indexOf("?") > 0) {
				throw new JdbcWrapperException("Error ejecutando consulta. No se admiten parametros");
			}

			ps = getPreparedStatement(con, queryStr);
			rs = ps.executeQuery();
			Cursor cursor = new Cursor();
			cursor.setResultSet(rs);

			List<T> lista = new LinkedList<>();
			while (rs.next()) {
				lista.add(rowManager.mapRow(cursor));
			}
			return lista;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta", e);
		} finally {
			close(rs);
			close(ps);
			if (isAutoClose()) {
				close(con);
			}
		}
	}

	@Override
	public int update(String updateStr, NamedParameterManager paramManager) {

		PreparedStatement ps = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando update. No hay conexión a BB.DD.");
			}

			if (updateStr == null) {
				throw new JdbcWrapperException("Error ejecutando update. No se ha definido la sentencia update");
			}

			if (paramManager == null) {
				throw new JdbcWrapperException("Error ejecutando update. Es necesario definir un gestor de parámetros");
			}

			ClassWrapper<String> wrap = new ClassWrapper<>();
			wrap.setValue(updateStr);
			List<String> fields = JdbcUtils.extractFields(wrap);
			ps = getPreparedStatement(con, wrap.getValue());
			NamedParameterSetter setter = new NamedParameterSetter();
			setter.setStatement(ps);
			setter.setFields(fields);
			paramManager.configureParameters(setter);
			
			int rows = ps.executeUpdate();

			if (isAutoCommit()) {
				commit(con);
			}
			return rows;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando update", e);
		} finally {
			close(ps);
			if (isAutoClose()) {
				close(con);
			}
		}
	}

	@Override
	public void delete(String deleteStr, NamedParameterManager paramManager) {

		PreparedStatement ps = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando delete. No hay conexión a BB.DD.");
			}

			if (deleteStr == null) {
				throw new JdbcWrapperException("Error ejecutando delete. No se ha definido la sentencia update");
			}

			if (paramManager == null) {
				throw new JdbcWrapperException("Error ejecutando delete. Es necesario definir un gestor de parámetros");
			}

			ClassWrapper<String> wrap = new ClassWrapper<>();
			wrap.setValue(deleteStr);
			List<String> fields = JdbcUtils.extractFields(wrap);
			ps = getPreparedStatement(con, wrap.getValue());
			NamedParameterSetter setter = new NamedParameterSetter();
			setter.setStatement(ps);
			setter.setFields(fields);
			paramManager.configureParameters(setter);

			
			ps.execute();
			if (isAutoCommit()) {
				commit(con);
			}

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando delete", e);
		} finally {
			close(ps);
			if (isAutoClose()) {
				close(con);
			}
		}

	}

	@Override
	public void insert(String insertStr, NamedParameterManager paramManager) {

		PreparedStatement ps = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando insert. No hay conexión a BB.DD.");
			}

			if (insertStr == null) {
				throw new JdbcWrapperException("Error ejecutando insert. No se ha definido la sentencia update");
			}

			if (paramManager == null) {
				throw new JdbcWrapperException("Error ejecutando insert. Es necesario definir un gestor de parámetros");
			}

			ClassWrapper<String> wrap = new ClassWrapper<>();
			wrap.setValue(insertStr);
			List<String> fields = JdbcUtils.extractFields(wrap);
			ps = getPreparedStatement(con, wrap.getValue());
			NamedParameterSetter setter = new NamedParameterSetter();
			setter.setStatement(ps);
			setter.setFields(fields);
			paramManager.configureParameters(setter);
			
			ps.execute();
			if (isAutoCommit()) {
				commit(con);
			}

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando insert", e);
		} finally {
			close(ps);
			if (isAutoClose()) {
				close(con);
			}
		}

	}

	@Override
	public int count(String queryCountStr) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. No hay conexión a BB.DD.");
			}

			if (queryCountStr == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. No se ha definido la consulta");
			}

			if (queryCountStr.toUpperCase().indexOf("COUNT") < 0) {
				throw new JdbcWrapperException("Error ejecutando consulta. No es una consulta de tipo COUNT");
			}

			ps = getPreparedStatement(con, queryCountStr);
			ParameterManager paramManager = new NoParametersManagerImpl();
			ParameterSetter setter = new ParameterSetter();
			setter.setStatement(ps);
			paramManager.configureParameters(setter);
			rs = ps.executeQuery();
			CountRecordsRowManagerImpl rowManager = new CountRecordsRowManagerImpl();
			Cursor c = new Cursor();
			c.setResultSet(rs);
			if (rs.next()) {
				return rowManager.mapRow(c);
			} else {
				throw new JdbcWrapperException("No se han recuperado registros en la consulta tipo COUNT");
			}

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta", e);
		} finally {
			close(rs);
			close(ps);
			if (isAutoClose()) {
				close(con);
			}
		}

	}

	@Override
	public int count(String queryCountStr, NamedParameterManager paramManager) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. No hay conexión a BB.DD.");
			}

			if (queryCountStr == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. No se ha definido la consulta");
			}

			if (queryCountStr.toUpperCase().indexOf("COUNT") < 0) {
				throw new JdbcWrapperException("Error ejecutando consulta. No es una consulta de tipo COUNT");
			}

			ClassWrapper<String> wrap = new ClassWrapper<>();
			wrap.setValue(queryCountStr);
			List<String> fields = JdbcUtils.extractFields(wrap);
			ps = getPreparedStatement(con, wrap.getValue());
			NamedParameterSetter setter = new NamedParameterSetter();
			setter.setStatement(ps);
			setter.setFields(fields);
			paramManager.configureParameters(setter);

			rs = ps.executeQuery();
			CountRecordsRowManagerImpl rowManager = new CountRecordsRowManagerImpl();
			Cursor c = new Cursor();
			c.setResultSet(rs);
			if (rs.next()) {
				return rowManager.mapRow(c);
			} else {
				throw new JdbcWrapperException("No se han recuperado registros en la consulta tipo COUNT");
			}

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta", e);
		} finally {
			close(rs);
			close(ps);
			if (isAutoClose()) {
				close(con);
			}
		}

	}

	@Override
	public JdbcNamedQuery<T> getQuery(String namedQueryStr) {

		PreparedStatement ps = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error generando consulta inline. No hay conexión a BB.DD.");
			}

			if (namedQueryStr == null) {
				throw new JdbcWrapperException("Error generando consulta inline. No se ha definido la consulta");
			}

			ClassWrapper<String> wrap = new ClassWrapper<>();
			wrap.setValue(namedQueryStr);
			List<String> fields = JdbcUtils.extractFields(wrap);
			ps = getPreparedStatement(con, wrap.getValue());
			NamedParameterSetter setter = new NamedParameterSetter();
			setter.setStatement(ps);
			setter.setFields(fields);

			JdbcNamedQuery<T> execQuery = new JdbcNamedQuery<T>(ps);
			execQuery.configureParameters(setter);
			return execQuery;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error generando consulta inline", e);
		} finally {
			// En este caso no cerramos nada, se cierra despues, tras leer el cursor...
		}
	}

	@Override
	public T getObject(String namedQueryStr, NamedParameterManager paramManager, RowManagerLambda<T> rowManager) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando consulta de registro único. No hay conexión a BB.DD.");
			}

			if (namedQueryStr == null) {
				throw new JdbcWrapperException(
						"Error ejecutando  consulta de registro único. No se ha definido la consulta");
			}

			if (paramManager == null) {
				throw new JdbcWrapperException(
						"Error ejecutando  consulta de registro único. Es necesario definir un gestor de parámetros");
			}

			if (rowManager == null) {
				throw new JdbcWrapperException(
						"Error ejecutando  consulta de registro único. Es necesario definir un gestor de filas");
			}

			ClassWrapper<String> wrap = new ClassWrapper<>();
			wrap.setValue(namedQueryStr);
			List<String> fields = JdbcUtils.extractFields(wrap);
			ps = getPreparedStatement(con, wrap.getValue());
			NamedParameterSetter setter = new NamedParameterSetter();
			setter.setStatement(ps);
			setter.setFields(fields);
			paramManager.configureParameters(setter);
			rs = ps.executeQuery();
			Cursor cursor = new Cursor();
			cursor.setResultSet(rs);

			if (rs.next()) {
				return rowManager.mapRow(cursor);
			} else {
				throw new JdbcWrapperException(
						"Error ejecutando consulta de registro único. No se han encontrado registros");
			}

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta", e);
		} finally {
			close(rs);
			close(ps);
			if (isAutoClose()) {
				close(con);
			}
		}
	}



}
