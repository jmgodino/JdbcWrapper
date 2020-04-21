package com.picoto.jdbc.wrapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

public class Campo {

	private String nombre;
	private String className;

	public Campo(String nombre, String className) {
		super();
		setNombre(nombre);
		setClassName(className);
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public String getClassName() {
		if ("java.sql.Date".equalsIgnoreCase(className)) {
			return "java.util.Date";
		} else if ("java.sql.Clob".equalsIgnoreCase(className)) {
			return "java.lang.String";
		} else if ("java.sql.Blob".equalsIgnoreCase(className)) {
			return "byte[]";
		} else {
			return className;
		}
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public Object getValor(int posicion, Cursor c) throws IOException, SQLException {
		switch (className) {
		case "java.lang.String":
			return c.getString(posicion);
		case "java.lang.Integer":
		case "int":
			return c.getInt(posicion);
		case "long":
		case "java.lang.Long":
		case "java.math.BigInteger":
			return c.getBigInt(posicion);
		case "java.math.BigDecimal":
		case "float":
		case "double":
			return c.getBigDecimal(posicion);
		case "java.sql.Date":
			return new Date(c.getDate(posicion).getTime());
		case "java.sql.Clob":
			return JdbcUtils.readFully(c.getClob(posicion).getAsciiStream());
		case "java.sql.Blob":
			return JdbcUtils.readFullyBinary(c.getBlob(posicion).getBinaryStream());
		default:
			return c.getString(posicion);
		}
	}

}
