package com.picoto.jdbc.wrapper;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.util.Date;

public class JdbcNamedQuery<T> extends JdbcBaseQuery<T> {

	private NamedParameterSetter namedParameterSetter;

	public JdbcNamedQuery(PreparedStatement ps) {
		namedParameterSetter = new NamedParameterSetter();
		namedParameterSetter.setStatement(ps);
	}

	public JdbcNamedQuery<T> parameterInteger(String name, int valor) {
		namedParameterSetter.setInt(name, valor);
		return this;
	}
	
	public JdbcNamedQuery<T> parameterBigInt(String name, long valor) {
		namedParameterSetter.setBigInt(name, valor);
		return this;
	}	

	public JdbcNamedQuery<T> parameterString(String name, String valor) {
		namedParameterSetter.setString(name, valor);
		return this;
	}

	public JdbcNamedQuery<T> parameterDate(String name, Date valor) {
		namedParameterSetter.setDate(name, valor);
		return this;
	}

	public JdbcNamedQuery<T> parameterBigDecimal(String name, BigDecimal valor) {
		namedParameterSetter.setBigDecimal(name, valor);
		return this;
	}

	public JdbcNamedQuery<T> parameterClob(String name, String valor) {
		namedParameterSetter.setClob(name, valor);
		return this;
	}

	public JdbcNamedQuery<T> parameterBlob(String name, byte[] valor) {
		namedParameterSetter.setBlob(name, valor);
		return this;
	}

	@Override
	protected PreparedStatement getStatement() {
		return namedParameterSetter.getStatement();
	}

	public void configureParameters(NamedParameterSetter setter) {
		this.namedParameterSetter = setter;

	}

}
