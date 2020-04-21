package com.picoto.jdbc.wrapper;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Timestamp;
import java.util.Date;

public class ProcedureOutputParameterGetter {

	private CallableStatement callableStatement;

	public CallableStatement getCallableStatement() {
		return callableStatement;
	}

	public void setCallableStatement(CallableStatement callableStatement) {
		this.callableStatement = callableStatement;
	}

	public int getInt(int pos) {
		try {
			return callableStatement.getInt(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Integer %s en el cursor", pos),
					e);
		}
	}

	public long getBigInt(int pos) {
		try {
			return callableStatement.getLong(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo BigInteger %s en el cursor", pos),
					e);
		}
	}
	
	public String getString(int pos) {
		try {
			return callableStatement.getString(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo String %s en el cursor", pos), e);
		}
	}

	public BigDecimal getBigDecimal(int pos) {
		try {
			return callableStatement.getBigDecimal(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo BigDecimal %s en el cursor", pos),
					e);
		}
	}

	public Clob getClob(int pos) {
		try {
			return callableStatement.getClob(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Clob %s en el cursor", pos), e);
		}
	}

	public String getClobAsString(int pos) {
		try {
			Clob clob = callableStatement.getClob(pos);
			return new String(JdbcUtils.readFully(clob.getAsciiStream()));
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Clob %s en el cursor", pos), e);
		}
	}

	public Blob getBlob(int pos) {
		try {
			return callableStatement.getBlob(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Blob %s en el cursor", pos), e);
		}
	}

	public Date getDate(int pos) {
		try {
			return callableStatement.getDate(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Date %s en el cursor", pos), e);
		}
	}

	public Timestamp getTimestamp(int pos) {
		try {
			return callableStatement.getTimestamp(pos);
		} catch (Exception e) {
			throw new JdbcWrapperException(String.format("Error al recuperar el campo Timestamp %s en el cursor", pos),
					e);
		}
	}
}
