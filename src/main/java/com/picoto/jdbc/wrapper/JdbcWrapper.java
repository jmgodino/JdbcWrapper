package com.picoto.jdbc.wrapper;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JdbcWrapper<T> {

	private Connection con;

	public Connection getConnection() {
		return con;
	}

	public void setConnection(Connection con) {
		this.con = con;
	}

	public List<T> query(String queryStr, ParameterManager paramManager, RowManagerLambda<T> rowManager) {
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

			PreparedStatement ps = getPreparedStatement(queryStr);
			ParameterSetter setter = new ParameterSetter();
			setter.setStatement(ps);
			paramManager.configureParameters(setter);
			ResultSet rs = ps.executeQuery();
			Cursor cursor = new Cursor();
			cursor.setResultSet(rs);
			List<T> lista = rowManager.mapRow(cursor);
			close(rs);
			close(ps);
			return lista;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta", e);
		}
	}
	
	public List<T> query(String queryStr, RowManagerLambda<T> rowManager) {
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

			PreparedStatement ps = getPreparedStatement(queryStr);
			ResultSet rs = ps.executeQuery();
			Cursor cursor = new Cursor();
			cursor.setResultSet(rs);
			List<T> lista = rowManager.mapRow(cursor);
			close(rs);
			close(ps);
			return lista;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta", e);
		}
	}

	public int update(String updateStr, ParameterManager paramManager) {
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

			PreparedStatement ps = getPreparedStatement(updateStr);
			ParameterSetter setter = new ParameterSetter();
			setter.setStatement(ps);
			paramManager.configureParameters(setter);
			int rows = ps.executeUpdate();
			close(ps);
			return rows;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando update", e);
		}
	}


	public void insert(String updateStr, ParameterManager paramManager) {
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

			PreparedStatement ps = getPreparedStatement(updateStr);
			ParameterSetter setter = new ParameterSetter();
			setter.setStatement(ps);
			paramManager.configureParameters(setter);
			ps.execute();
			close(ps);
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando insert", e);
		}
	}

	public int count(String queryCountStr) {
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

			PreparedStatement ps = getPreparedStatement(queryCountStr);
			ParameterManager paramManager = new NoParametersManagerImpl();
			ParameterSetter setter = new ParameterSetter();
			setter.setStatement(ps);
			paramManager.configureParameters(setter);
			ResultSet rs = ps.executeQuery();
			CountRecordsRowManagerImpl rowManager = new CountRecordsRowManagerImpl();
			Cursor c = new Cursor();
			c.setResultSet(rs);
			List<Integer> lista = rowManager.mapRow(c);
			close(rs);
			close(ps);
			return lista.get(0);
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta", e);
		}

	}

	public void commit(Connection con) {
		try {
			if (con != null) {
				con.commit();
			}
		} catch (Exception e) {
			throw new JdbcWrapperException("Error haciendo commit", e);
		}
	}

	public void rollback(Connection con) {
		try {
			if (con != null) {
				con.rollback();
			}
		} catch (Exception e) {
			throw new JdbcWrapperException("Error haciendo rollback", e);
		}
	}

	protected PreparedStatement getPreparedStatement(String query) {
		try {
			PreparedStatement ps = con.prepareStatement(query);
			return ps;
		} catch (Exception e) {
			debug(String.format("Error creando statement: %s", query));
			throw new JdbcWrapperException(query, e);
		}
	}

	protected CallableStatement getCallableStatement(String query, boolean isFunction) {
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

	protected void close(ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (Exception e) {
			debug("Error cerrando cursor");
		}
	}

	protected void close(Statement stmt) {
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (Exception e) {
			debug("Error cerrando sentencia");
		}
	}

	public static void close(Connection con) {
		try {
			if (con != null) {
				con.close();
			}
		} catch (Exception e) {
			debug("Error cerrando conexion");
		}
	}

	public static void debug(String msg) {
		System.out.println(msg);
	}

	public List<T> namedQuery(String namedQueryStr, NamedParameterManager paramManager,
			RowManagerLambda<T> rowManager) {
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

			List<String> fields = new ArrayList<String>();
			int pos;

			while ((pos = namedQueryStr.indexOf(":")) != -1) {
				int end = namedQueryStr.substring(pos).indexOf(" ");
				if (end == -1)
					end = namedQueryStr.length();
				else
					end += pos;
				fields.add(namedQueryStr.substring(pos + 1, end));
				namedQueryStr = namedQueryStr.substring(0, pos) + "?" + namedQueryStr.substring(end);
			}

			PreparedStatement ps = getPreparedStatement(namedQueryStr);
			NamedParameterSetter setter = new NamedParameterSetter();
			setter.setStatement(ps);
			setter.setFields(fields);
			paramManager.configureParameters(setter);
			ResultSet rs = ps.executeQuery();
			Cursor c = new Cursor();
			c.setResultSet(rs);
			List<T> lista = rowManager.mapRow(c);
			close(rs);
			close(ps);
			return lista;
		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando named query", e);
		}
	}

	public void callProcedure(String procedureStr, ProcedureParameterManager paramManager,
			ProcedureOutputParameterManager resultManager, boolean isFunction) {

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

			CallableStatement cs = getCallableStatement(procedureStr, isFunction);

			ProcedureParameterSetter setter = new ProcedureParameterSetter();
			setter.setCallableStatement(cs);
			paramManager.configureParameters(setter);
			cs.execute();
			ProcedureOutputParameterGetter getter = new ProcedureOutputParameterGetter();
			getter.setCallableStatement(cs);
			resultManager.getOutputParameters(getter);

			close(cs);

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando PA", e);
		}

	}

}
