package com.picoto.jdbc.wrapper;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.util.Date;

public class JdbcQuery<T> extends JdbcBaseQuery<T> {

	private int currentIndex = 1;
	private ParameterSetter paramSetter;

	public JdbcQuery(PreparedStatement ps) {
		paramSetter = new ParameterSetter();
		paramSetter.setStatement(ps);
	}

	public JdbcQuery<T> parameterInteger(int valor) {
		paramSetter.setInt(currentIndex++, valor);
		return this;
	}

	public JdbcQuery<T> parameterString(String valor) {
		paramSetter.setString(currentIndex++, valor);
		return this;
	}

	public JdbcQuery<T> parameterDate(Date valor) {
		paramSetter.setDate(currentIndex++, valor);
		return this;
	}

	public JdbcQuery<T> parameterBigDecimal(BigDecimal valor) {
		paramSetter.setBigDecimal(currentIndex++, valor);
		return this;
	}

	public JdbcQuery<T> parameterClob(String valor) {
		paramSetter.setClob(currentIndex++, valor);
		return this;
	}

	public JdbcQuery<T> parameterBlob(byte[] valor) {
		paramSetter.setBlob(currentIndex++, valor);
		return this;
	}

	@Override
	protected PreparedStatement getStatement() {
		return paramSetter.getStatement();
	}

}
