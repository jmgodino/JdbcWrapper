package com.picoto.jdbc.wrapper;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

public class JdbcWrapper<T> extends JdbcBase {

	private DataSource ds;
	private boolean autoCommit = false;

	public DataSource getDataSource() {
		return ds;
	}

	public void setDataSource(DataSource ds) {
		if (ds == null) {
			throw new JdbcWrapperException("No se ha incluido un DataSource válido");
		}
		this.ds = ds;
	}

	public void setDataSource(DataSource ds, boolean autoCommit) {
		setDataSource(ds);
		this.autoCommit = autoCommit;
	}

	public List<T> query(String queryStr, ParameterManager paramManager, RowManagerLambda<T> rowManager) {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = getConnection();
			if (ds == null) {
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
			List<T> lista = rowManager.mapRow(cursor);
			return lista;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta", e);
		} finally {
			close(rs);
			close(ps);
			close(con);
		}
	}

	public List<T> query(String queryStr, RowManagerLambda<T> rowManager) {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = getConnection();
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
			List<T> lista = rowManager.mapRow(cursor);
			return lista;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta", e);
		} finally {
			close(rs);
			close(ps);
			close(con);
		}
	}

	public int update(String updateStr, ParameterManager paramManager) {
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = getConnection();
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
			return rows;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando update", e);
		} finally {
			close(ps);
			close(con);
		}
	}

	private Connection getConnection() throws SQLException {
		Connection con = ds.getConnection();
		if (autoCommit) {
			con.setAutoCommit(true);
		}
		return con;
	}

	public void delete(String updateStr, ParameterManager paramManager) {
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = getConnection();
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

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando delete", e);
		} finally {
			close(ps);
			close(con);
		}

	}

	public void insert(String updateStr, ParameterManager paramManager) {
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = getConnection();

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

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando insert", e);
		} finally {
			close(ps);
			close(con);
		}

	}

	public int count(String queryCountStr) {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = getConnection();
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
			List<Integer> lista = rowManager.mapRow(c);
			return lista.get(0);
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta", e);
		} finally {
			close(rs);
			close(ps);
			close(con);
		}

	}

	protected PreparedStatement getPreparedStatement(Connection con, String query) {
		try {
			PreparedStatement ps = con.prepareStatement(query);
			return ps;
		} catch (Exception e) {
			debug(String.format("Error creando statement: %s", query));
			throw new JdbcWrapperException(query, e);
		}
	}

	protected CallableStatement getCallableStatement(Connection con, String query, boolean isFunction) {
		try {
			CallableStatement cs = null;
			if (isFunction) {
				cs = con.prepareCall("{? = call " + query + "}");
			} else {
				cs = con.prepareCall("{call " + query + "}");
			}
			return cs;
		} catch (Exception e) {
			debug(String.format("Error creando PA: %s", query));
			throw new JdbcWrapperException(query, e);
		}
	}

	public List<T> namedQuery(String namedQueryStr, NamedParameterManager paramManager,
			RowManagerLambda<T> rowManager) {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = getConnection();
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

			ps = getPreparedStatement(con, namedQueryStr);
			NamedParameterSetter setter = new NamedParameterSetter();
			setter.setStatement(ps);
			setter.setFields(JdbcUtils.extractFields(namedQueryStr));
			paramManager.configureParameters(setter);
			rs = ps.executeQuery();
			Cursor c = new Cursor();
			c.setResultSet(rs);
			List<T> lista = rowManager.mapRow(c);
			return lista;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando named query", e);
		} finally {
			close(rs);
			close(ps);
			close(con);
		}
	}

	public void callProcedure(String procedureStr, ProcedureParameterManager paramManager,
			ProcedureOutputParameterManager resultManager, boolean isFunction) {
		Connection con = null;
		CallableStatement cs = null;
		try {
			con = getConnection();
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

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando PA", e);
		} finally {
			close(cs);
			close(con);
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
	public JdbcQuery<T> getQuery(String namedQueryStr) {
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = getConnection();
			if (con == null) {
				throw new JdbcWrapperException("Error generando consulta inline. No hay conexión a BB.DD.");
			}

			if (namedQueryStr == null) {
				throw new JdbcWrapperException("Error generando consulta inline. No se ha definido la consulta");
			}

			ps = getPreparedStatement(con, namedQueryStr);
			JdbcQuery<T> execQuery = new JdbcQuery<T>(ps);
			return execQuery;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error generando consulta inline", e);
		} finally {
			// En este caso no cerramos nada...
		}
	}

}
