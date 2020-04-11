package com.picoto.jdbc.wrapper;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

public class ParameterSetter {
	
	private PreparedStatement statement;

	public PreparedStatement getStatement() {
		return statement;
	}

	public void setStatement(PreparedStatement statement) {
		this.statement = statement;
	}

	public void setInt(int pos, int valor) {
		try {
			statement.setInt(pos, valor);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Integer %d %s", pos, valor), e);
		}
	}

	public void setString(int pos, String valor) {
		try {
			statement.setString(pos, valor);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro String %d %s", pos, valor), e);
		}
	}

	public void setBigDecimal(int pos, BigDecimal valor) {
		try {
			statement.setBigDecimal(pos, valor);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro BigDecimal %d %s", pos, valor), e);
		}
	}

	public void setDate(int pos, Date valor) {
		try {
			statement.setDate(pos, new java.sql.Date(valor.getTime()));
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Date %d %s", pos, valor), e);
		}
	}

	public void setBlob(int pos, byte[] valor) {
		try {
			//TODO: A revisar en Oracle
			statement.setBinaryStream(pos, new ByteArrayInputStream(valor));
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Blob %d %s", pos, valor), e);
		}
	}

	public void setClob(int pos, String valor) {
		try {
			//TODO: A revisar en Oracle
			statement.setAsciiStream(pos, new ByteArrayInputStream(valor.getBytes()));
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Clob %d %s", pos, valor), e);
		}
	}

}
