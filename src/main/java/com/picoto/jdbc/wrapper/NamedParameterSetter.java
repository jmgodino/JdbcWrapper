package com.picoto.jdbc.wrapper;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

public class NamedParameterSetter {
	
	private PreparedStatement statement;

	private List<String> fields;

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	public PreparedStatement getStatement() {
		return statement;
	}

	public void setStatement(PreparedStatement statement) {
		this.statement = statement;
	}

	public void setInt(String name, int valor) {
		try {
			statement.setInt(getIndex(name), valor);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Integer %s %s", name, valor), e);
		}
	}

	public void setBigInt(String name, long valor) {
		try {
			statement.setLong(getIndex(name), valor);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro BigInteger %s %s", name, valor), e);
		}
	}
	
	public void setString(String name, String valor) {
		try {
			statement.setString(getIndex(name), valor);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro String %s %s", name, valor), e);
		}
	}

	public void setBigDecimal(String name, BigDecimal valor) {
		try {
			statement.setBigDecimal(getIndex(name), valor);
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro BigDecimal %d %s", name, valor),
					e);
		}
	}

	public void setDate(String name, Date valor) {
		try {
			statement.setDate(getIndex(name), new java.sql.Date(valor.getTime()));
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Date %d %s", name, valor), e);
		}
	}

	public void setBlob(String name, byte[] valor) {
		try {
			statement.setBinaryStream(getIndex(name), new ByteArrayInputStream(valor));
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Blob %d %s", name, valor), e);
		}
	}

	public void setClob(String name, String valor) {
		try {
			statement.setAsciiStream(getIndex(name), new ByteArrayInputStream(valor.getBytes()));
		} catch (SQLException e) {
			throw new JdbcWrapperException(String.format("Error preparando parametro Clob %d %s", name, valor), e);
		}
	}

	private int getIndex(String name) {
		return fields.indexOf(name) + 1;
	}


}
