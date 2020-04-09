package com.picoto.jdbc.wrapper;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

public class ProcedureParameterSetter {

	CallableStatement statement;

	public CallableStatement getCallableStatement() {
		return statement;
	}

	public void setCallableStatement(CallableStatement cs) {
		this.statement = cs;
	}

	public void registrarEntradaInt(int pos, int valor) {
		try {
			statement.setInt(pos, valor);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Integer %d %s", pos, valor), e);
		}
	}

	public void registrarEntradaString(int pos, String valor) {
		try {
			statement.setString(pos, valor);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro String %d %s", pos, valor), e);
		}
	}

	public void registrarEntradaBigDecimal(int pos, BigDecimal valor) {
		try {
			statement.setBigDecimal(pos, valor);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro BigDecimal %d %s", pos, valor), e);
		}
	}

	public void registrarEntradaDate(int pos, Date valor) {
		try {
			statement.setDate(pos, new java.sql.Date(valor.getTime()));
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Date %d %s", pos, valor), e);
		}
	}

	public void registrarEntradaBlob(int pos, byte[] valor) {
		try {
			statement.setBinaryStream(pos, new ByteArrayInputStream(valor));
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Blob %d %s", pos, valor), e);
		}
	}

	public void registrarEntradaClob(int pos, String valor) {
		try {
			statement.setAsciiStream(pos, new ByteArrayInputStream(valor.getBytes()));
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Clob %d %s", pos, valor), e);
		}
	}

	public void registrarSalidaInt(int pos) {
		try {
			statement.registerOutParameter(pos, Types.INTEGER);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro salida Int %s", pos), e);
		}
	}

	public void registrarSalidaString(int pos) {
		try {
			statement.registerOutParameter(pos, Types.VARCHAR);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro salida String %s", pos), e);
		}
	}

	public void registrarSalidaBigDecimal(int pos) {
		try {
			statement.registerOutParameter(pos, Types.BIGINT);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro salida BigDecimal %s", pos), e);
		}
	}

	public void registrarSalidaDate(int pos) {
		try {
			statement.registerOutParameter(pos, Types.DATE);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro salida Date %s", pos), e);
		}
	}

	public void registrarSalidaClob(int pos) {
		try {
			statement.registerOutParameter(pos, Types.CLOB);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro salida CLOB %s", pos), e);
		}
	}

	public void registrarSalidaBlob(int pos) {
		try {
			statement.registerOutParameter(pos, Types.BLOB);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro salida BLOB %s", pos), e);
		}
	}
	
}
