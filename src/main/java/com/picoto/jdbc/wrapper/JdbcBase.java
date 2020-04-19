package com.picoto.jdbc.wrapper;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcBase {
	
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
			customDebug("Error cerrando conexion");
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


	public void debug(String msg) {
		customDebug(msg);
	}
	
	static void customDebug(String msg) {
		System.out.println(msg);
	}

}
