package com.picoto.jdbc.wrapper;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;

public class Campo {

	public enum TIPO {
		STRING, INTEGER, BIGDECIMAL, DATE, CLOB, BLOB
	};

	private String nombre;
	private TIPO tipo;

	public Campo(String nombre, TIPO tipo) {
		super();
		this.nombre = nombre;
		this.tipo = tipo;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public TIPO getTipo() {
		return tipo;
	}

	public void setTipo(TIPO tipo) {
		this.tipo = tipo;
	}

	public static TIPO map(int tipo) {
		switch (tipo) {
		case Types.CHAR:
			return TIPO.STRING;
		case Types.INTEGER:
		case Types.SMALLINT:
			return TIPO.INTEGER;
		case Types.DATE:
		case Types.TIMESTAMP:
			return TIPO.DATE;
		case Types.DECIMAL:
		case Types.BIGINT:
		case Types.FLOAT:
		case Types.DOUBLE:
			return TIPO.BIGDECIMAL;
		case Types.CLOB:
			return TIPO.CLOB;
		case Types.BLOB:
			return TIPO.BLOB;
		default:
			return TIPO.STRING;
		}

	}

	public Class<?> getClassType() throws ClassNotFoundException {
		switch (tipo) {
		case STRING:
			return Class.forName("java.lang.String");
		case INTEGER:
			return Class.forName("java.lang.Integer");
		case BIGDECIMAL:
			return Class.forName("java.math.BigDecimal");
		case DATE:
			return Class.forName("java.util.Date");
		case CLOB:
			return Class.forName("java.lang.String");
		case BLOB:
			return Class.forName("java.lang.Byte");
		default:
			return Class.forName("java.lang.String");
		}
	}

	public Object getValor(int posicion, Cursor c) throws IOException, SQLException {
		switch (tipo) {
		case STRING:
			return c.getString(posicion);
		case INTEGER:
			return c.getInt(posicion);
		case BIGDECIMAL:
			return c.getBigDecimal(posicion);
		case DATE:
			return c.getDate(posicion);
		case CLOB:
			return JdbcUtils.readFully(c.getClob(posicion).getAsciiStream());
		case BLOB:
			return JdbcUtils.readFullyBinary(c.getBlob(posicion).getBinaryStream());
		default: return c.getString(posicion);
		}
	}

}
