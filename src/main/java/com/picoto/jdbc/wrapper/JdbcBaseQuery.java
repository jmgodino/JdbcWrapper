package com.picoto.jdbc.wrapper;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class JdbcBaseQuery<T>  extends JdbcBase {


	protected abstract PreparedStatement getStatement();
	

	public T getObject(Class<T> clase) {
		ResultSet rs = null;
		PreparedStatement ps = getStatement();
		try {
			rs = ps.executeQuery();
			Cursor cursor = new Cursor();
			cursor.setResultSet(rs);
			return mapOneRow(cursor, clase);

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta inline", e);
		} finally {
			close(rs);
			close(ps);
		}
	}

	public T getMappedObject(RowManagerLambda<T> rowManager) {
		ResultSet rs = null;
		PreparedStatement ps = getStatement();
		try {
			rs = ps.executeQuery();
			Cursor cursor = new Cursor();
			cursor.setResultSet(rs);
			if (rs.next()) {
				return rowManager.mapRow(cursor);
			} else {
				throw new JdbcWrapperException("No se han encontrado el registro");
			}

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta inline", e);
		} finally {
			close(rs);
			close(ps);
		}
	}

	public T mapOneRow(Cursor c, Class<T> clase)
			throws InstantiationException, IllegalAccessException, SQLException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException, IOException {

		List<Campo> camposQuery = getCamposQuery(c.getResultSet());
		Map<Campo, Method> mapaCampos = getMapeoCampos(camposQuery, clase);

		if (c.next()) {
			T instancia = getNewInstance(clase);

			for (int i = 0; i < camposQuery.size(); i++) {
				Campo campo = camposQuery.get(i);
				mapaCampos.get(campo).invoke(instancia, campo.getValor(i + 1, c));
			}

			return instancia;
		} else {
			throw new JdbcWrapperException("No se han encontrado el registro");
		}

	}

	public List<T> mapRow(Cursor c, Class<T> clase)
			throws InstantiationException, IllegalAccessException, SQLException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException, IOException {

		List<T> valores = new ArrayList<T>();
		List<Campo> camposQuery = getCamposQuery(c.getResultSet());
		Map<Campo, Method> mapaCampos = getMapeoCampos(camposQuery, clase);

		while (c.next()) {
			T instancia = getNewInstance(clase);

			for (int i = 0; i < camposQuery.size(); i++) {
				Campo campo = camposQuery.get(i);
				mapaCampos.get(campo).invoke(instancia, campo.getValor(i + 1, c));
			}

			valores.add(instancia);
		}
		return valores;
	}

	private T getNewInstance(Class<T> clase) {
		try {
			T instancia = (T) clase.newInstance();
			return instancia;
		} catch (Exception e) {
			throw new JdbcWrapperException("No se ha podido instanciar la clase ".concat(clase.getCanonicalName())
					.concat(" ¿Tiene un constructor sin parámetros?"));
		}
	}

	private List<Campo> getCamposQuery(ResultSet rs) throws SQLException {

		List<Campo> listaCampos = new ArrayList<Campo>(25);
		ResultSetMetaData rsmd = rs.getMetaData();

		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			debug("Campo en la query: " + rsmd.getColumnName(i) + " Tipo: " + rsmd.getColumnClassName(i));
			listaCampos.add(new Campo(rsmd.getColumnName(i), rsmd.getColumnClassName(i)));

		}
		return listaCampos;
	}

	private Map<Campo, Method> getMapeoCampos(List<Campo> camposQuery, Class<T> clase) {
		Map<Campo, Method> mapeo = new HashMap<>();
		for (Campo campo : camposQuery) {
			mapeo.put(campo, getMetodo(clase, campo));
		}
		return mapeo;
	}

	private Method getMetodo(Class<T> clase, Campo campo) {
		String nombreMetodo = "set".concat(JdbcUtils.capitalize(campo.getNombre()));
		try {
			return clase.getDeclaredMethod(nombreMetodo, Class.forName(campo.getClassName()));
		} catch (Exception e) {
			throw new JdbcWrapperException("No se ha encontrado el metodo ".concat(nombreMetodo)
					.concat(" en la clase ".concat(clase.getCanonicalName())), e);
		}
	}
	
	public List<T> executeQuery(Class<T> clase) {
		ResultSet rs = null;
		PreparedStatement ps = getStatement();
		try {
			rs = ps.executeQuery();
			Cursor cursor = new Cursor();
			cursor.setResultSet(rs);
			return mapRow(cursor, clase);

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta inline", e);
		} finally {
			close(rs);
			close(ps);
		}
	}

	public List<T> executeMappedQuery(RowManagerLambda<T> rowManager) {
		ResultSet rs = null;
		PreparedStatement ps = getStatement();
		try {
			rs = ps.executeQuery();
			Cursor cursor = new Cursor();
			cursor.setResultSet(rs);
			List<T> lista = new LinkedList<>();
			while (rs.next()) {
				lista.add(rowManager.mapRow(cursor));
			}
			return lista;

		} catch (Exception e) {
			throw new JdbcWrapperException("Error ejecutando consulta inline con rowManager", e);
		} finally {
			close(rs);
			close(ps);
		}
	}

}
