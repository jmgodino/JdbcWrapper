package com.picoto.jdbc.wrapper;

import java.sql.Connection;
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
			debug("Error cerrando conexion");
		}
	}
	

	public static void debug(String msg) {
		System.out.println(msg);
	}

}
