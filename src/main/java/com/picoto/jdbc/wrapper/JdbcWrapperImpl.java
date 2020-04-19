package com.picoto.jdbc.wrapper;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

public class JdbcWrapperImpl<T> extends JdbcBase implements JdbcWrapper<T>  {

	private Connection con;
	private boolean autoCommit = false;
	private boolean autoClose = false;

	public JdbcWrapperImpl() {
		super();
		// TODO Auto-generated constructor stub
	}

	public JdbcWrapperImpl(Connection con) {
		super();
		this.con = con;
		this.autoCommit = false;
		this.autoClose = false;
	}

	public JdbcWrapperImpl(Connection con, boolean autoCommit) {
		super();
		this.con = con;
		this.autoCommit = autoCommit;
		this.autoClose = false;
	}

	public JdbcWrapperImpl(Connection con, boolean autoCommit, boolean autoClose) {
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
	public List<T> query(String queryStr, ParameterManager paramManager, RowManagerLambda<T> rowManager) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. No hay conexión a BB.DD.");
			}

			if (queryStr == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. No se ha definido la consulta");
			}

			if (paramManager == null) {
				throw new JdbcWrapperException(
						"Error ejecutando consulta. Es necesario definir un gestor de parámetros");
			}

			if (rowManager == null) {
				throw new JdbcWrapperException("Error ejecutando consulta. Es necesario definir un gestor de filas");
			}

			ps = getPreparedStatement(con, queryStr);
			ParameterSetter setter = new ParameterSetter();
			setter.setStatement(ps);
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
	public int update(String updateStr, ParameterManager paramManager) {

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

			ps = getPreparedStatement(con, updateStr);
			ParameterSetter setter = new ParameterSetter();
			setter.setStatement(ps);
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
	public void delete(String updateStr, ParameterManager paramManager) {

		PreparedStatement ps = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando delete. No hay conexión a BB.DD.");
			}

			if (updateStr == null) {
				throw new JdbcWrapperException("Error ejecutando delete. No se ha definido la sentencia update");
			}

			if (paramManager == null) {
				throw new JdbcWrapperException("Error ejecutando delete. Es necesario definir un gestor de parámetros");
			}

			ps = getPreparedStatement(con, updateStr);
			ParameterSetter setter = new ParameterSetter();
			setter.setStatement(ps);
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
	public void insert(String updateStr, ParameterManager paramManager) {

		PreparedStatement ps = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando insert. No hay conexión a BB.DD.");
			}

			if (updateStr == null) {
				throw new JdbcWrapperException("Error ejecutando insert. No se ha definido la sentencia update");
			}

			if (paramManager == null) {
				throw new JdbcWrapperException("Error ejecutando insert. Es necesario definir un gestor de parámetros");
			}

			ps = getPreparedStatement(con, updateStr);
			ParameterSetter setter = new ParameterSetter();
			setter.setStatement(ps);
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
	public void callFunction(String procedureStr, ProcedureParameterManager paramManager,
			ProcedureOutputParameterManager resultManager) {
		callProcedure(procedureStr, paramManager, resultManager, true);
	}

	@Override
	public void callProcedure(String procedureStr, ProcedureParameterManager paramManager,
			ProcedureOutputParameterManager resultManager, boolean isFunction) {

		CallableStatement cs = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando PA. No hay conexión a BB.DD.");
			}

			if (procedureStr == null) {
				throw new JdbcWrapperException("Error ejecutando PA. No se ha definido el PA");
			}

			if (paramManager == null) {
				throw new JdbcWrapperException("Error ejecutando PA. Es necesario definir un gestor de parámetros");
			}

			if (resultManager == null) {
				throw new JdbcWrapperException(
						"Error ejecutando PA. Es necesario definir un gestor de parámetros de salida");
			}

			cs = getCallableStatement(con, procedureStr, isFunction);

			ProcedureParameterSetter setter = new ProcedureParameterSetter();
			setter.setCallableStatement(cs);
			paramManager.configureParameters(setter);
			cs.execute();
			ProcedureOutputParameterGetter getter = new ProcedureOutputParameterGetter();
			getter.setCallableStatement(cs);
			resultManager.getOutputParameters(getter);

			if (autoCommit) {
				commit(con);
			}

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando PA", e);
		} finally {
			close(cs);
			if (isAutoClose()) {
				close(con);
			}
		}

	}

	/**
	 * Este método necesita que los campos devueltos en la proyección de la consulta
	 * tengan el mismo nombre que los campos del objeto de salida. Además el objeto
	 * necesitará un constructor vacío. En caso contrario se producirá un error.
	 * 
	 * @param namedQueryStr
	 * @return
	 */
	@Override
	public JdbcQuery<T> getQuery(String queryStr) {

		PreparedStatement ps = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error generando consulta inline. No hay conexión a BB.DD.");
			}

			if (queryStr == null) {
				throw new JdbcWrapperException("Error generando consulta inline. No se ha definido la consulta");
			}

			ps = getPreparedStatement(con, queryStr);
			JdbcQuery<T> execQuery = new JdbcQuery<T>(ps);
			return execQuery;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error generando consulta inline", e);
		} finally {
			// En este caso no cerramos nada, se cierra despues, tras leer el cursor...
		}
	}

	@Override
	public T getRecord(String queryStr, ParameterManager paramManager, RowManagerLambda<T> rowManager) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {

			if (con == null) {
				throw new JdbcWrapperException("Error ejecutando consulta de registro único. No hay conexión a BB.DD.");
			}

			if (queryStr == null) {
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

			ps = getPreparedStatement(con, queryStr);
			ParameterSetter setter = new ParameterSetter();
			setter.setStatement(ps);
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
