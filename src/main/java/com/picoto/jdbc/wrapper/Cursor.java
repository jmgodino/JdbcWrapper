package com.picoto.jdbc.wrapper;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;

public class Cursor {

	private ResultSet resultSet;

	public Cursor() {
		super();
	}

	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	protected ResultSet getResultSet() {
		return resultSet;
	}

	public int getInt(int pos) {
		try {
			return resultSet.getInt(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Integer %s en el cursor", pos),
					e);
		}
	}

	public long getBigInt(int pos) {
		try {
			return resultSet.getLong(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo BigInteger %s en el cursor", pos),
					e);
		}
	}
	
	public String getString(int pos) {
		try {
			return resultSet.getString(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo String %s en el cursor", pos), e);
		}
	}

	public BigDecimal getBigDecimal(int pos) {
		try {
			return resultSet.getBigDecimal(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo BigDecimal %s en el cursor", pos),
					e);
		}
	}

	public Clob getClob(int pos) {
		try {
			return resultSet.getClob(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Clob %s en el cursor", pos), e);
		}
	}

	public String getClobAsString(int pos) {
		try {
			Clob clob = resultSet.getClob(pos);
			return new String(JdbcUtils.readFully(clob.getAsciiStream()));
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Clob %s en el cursor", pos), e);
		}
	}

	public Blob getBlob(int pos) {
		try {
			return resultSet.getBlob(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Blob %s en el cursor", pos), e);
		}
	}

	public Date getDate(int pos) {
		try {
			return resultSet.getDate(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Date %s en el cursor", pos), e);
		}
	}

	public Timestamp getTimestamp(int pos) {
		try {
			return resultSet.getTimestamp(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Timestamp %s en el cursor", pos),
					e);
		}
	}

	public boolean next() {
		try {
			return resultSet.next();
		} catch (Exception e) {
			throw new JdbcWrapperException("Error al interar en el cursor", e);
		}
	}

}
