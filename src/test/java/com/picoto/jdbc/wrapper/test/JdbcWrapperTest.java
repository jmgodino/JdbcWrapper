package com.picoto.jdbc.wrapper.test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.derby.tools.ij;

import com.picoto.jdbc.wrapper.ClassWrapper;
import com.picoto.jdbc.wrapper.JdbcWrapper;
import com.picoto.jdbc.wrapper.JdbcWrapperException;

import org.junit.Test;


public class JdbcWrapperTest {

	@Test
	public void runTest()  {

		Connection con = getDerbyConnection();

		JdbcWrapper.debug("************************************");
		JdbcWrapper.debug("Iniciando test");


		llamarPA(con);

		llamarPAFuncion(con);

		consultarLibro(con);

		consultaLibro2(con);

		contarLibros(con);

		consultarTodos(con);
		
		crearLibro(con);

		consultarTodos(con);
		
		actualizarLibro(con);
		
		consultarTodos(con);
		
		borrarLibro(con);

		consultarTodos(con);
		
		JdbcWrapper.close(con);


	}

	private Connection getDerbyConnection() {
		try {
			String driver = "org.apache.derby.jdbc.EmbeddedDriver";
			String connectionURL = "jdbc:derby:database/testDB;create=true";
			Class.forName(driver);
			Connection con = DriverManager.getConnection(connectionURL, "user", "pass");
			ij.runScript(con, new FileInputStream("database/create.sql"), "ISO-8859-1", new ByteArrayOutputStream(1),
					"ISO-8859-1");
			return con;
		} catch (Exception e) {
			throw new JdbcWrapperException("No hay conexión con BB.DD.");
		}
	}

	private void llamarPA(Connection con) {
		final ClassWrapper<String> wrapper = new ClassWrapper<String>();
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setConnection(con);
		testWrap.callProcedure("EJEMPLOPA(?,?)", cs -> {
			cs.registrarEntradaInt(1, 4);
			cs.registrarSalidaString(2);
		}, cso -> {
			wrapper.setValue(cso.getString(2));
		}, false);

		JdbcWrapper.debug("* Recuperar libro desde PA:  " + wrapper.getValue());

	}

	private void llamarPAFuncion(Connection con) {
		final ClassWrapper<String> wrapper = new ClassWrapper<String>();
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setConnection(con);
		testWrap.callProcedure("EJEMPLOPAFUNC(?)", cs -> {
			cs.registrarSalidaString(1);
			cs.registrarEntradaInt(2, 4);
		}, cso -> {
			wrapper.setValue(cso.getString(1));
		}, true);

		JdbcWrapper.debug("* Recuperar libro desde PA con format de funcion:  " + wrapper.getValue());

	}

	private void contarLibros(Connection con) {
		JdbcWrapper<Integer> testWrap = new JdbcWrapper<>();
		testWrap.setConnection(con);
		int total = testWrap.count("select count(*) from libros");

		JdbcWrapper.debug("* Contando libros");
		JdbcWrapper.debug("Total libros: " + total);
	}

	private void consultarLibro(Connection con) {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setConnection(con);
		List<Libro> libros = testWrap.query("select titulo, isbn, fecha, precio, texto from libros where isbn = ?",
				ps -> {
					ps.setInt(1, 2);
				}, c -> {

					List<Libro> lista = new ArrayList<Libro>(100);
					while (c.next()) {
						Libro l = new Libro();
						l.setTitulo(c.getString(1));
						l.setISBN(c.getInt(2));
						l.setFecha(c.getDate(3));
						l.setPrecio(c.getBigDecimal(4));
						l.setTexto(c.getClobAsString(5));
						lista.add(l);
					}
					return lista;
				});

		JdbcWrapper.debug("* Libros recuperados por ISBN");
		for (Libro l : libros) {
			JdbcWrapper.debug(l.toString());
		}

	}

	private void crearLibro(Connection con) {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<Libro>();
		testWrap.setConnection(con);
		testWrap.insert("insert into libros (isbn, titulo, fecha, precio, texto) values (?, ?, ?, ?, ?)", ps -> {
			ps.setInt(1, 3);
			ps.setString(2, "El Silmarilion");
			ps.setDate(3, new Date());
			ps.setBigDecimal(4, BigDecimal.valueOf(34.56f));
			ps.setClob(5, "Vaya rollo de libro");
		});
		testWrap.commit(con);
		JdbcWrapper.debug("* Libro creado");

	}

	private void actualizarLibro(Connection con) {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<Libro>();
		testWrap.setConnection(con);
		testWrap.insert("update libros set precio = 25.52 where titulo = ?", ps -> {
			ps.setString(1, "El Silmarilion");
		});
		testWrap.commit(con);
		JdbcWrapper.debug("* Libro actualizado");

	}

	private void borrarLibro(Connection con) {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<Libro>();
		testWrap.setConnection(con);
		testWrap.insert("delete from libros where titulo = ?", ps -> {
			ps.setString(1, "El Silmarilion");
		});
		testWrap.commit(con);
		JdbcWrapper.debug("* Libro borrado");

	}

	private void consultaLibro2(Connection con) {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setConnection(con);
		List<Libro> libros = testWrap.namedQuery(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = :isbn and titulo = :titulo", np -> {
					np.setInt("isbn", 1);
					np.setString("titulo", "El señor de los anillos");
				}, c -> {

					List<Libro> lista = new ArrayList<Libro>(100);
					while (c.next()) {
						Libro l = new Libro();
						l.setTitulo(c.getString(1));
						l.setISBN(c.getInt(2));
						l.setFecha(c.getDate(3));
						l.setPrecio(c.getBigDecimal(4));
						l.setTexto(c.getClobAsString(5));
						lista.add(l);
					}
					return lista;
				});

		JdbcWrapper.debug("* Libros recuperados por ISBN y título");
		for (Libro l : libros) {
			JdbcWrapper.debug(l.toString());
		}
	}

	private void consultarTodos(Connection con) {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setConnection(con);
		List<Libro> libros = testWrap.query("select titulo, isbn, fecha, precio, texto from libros", c -> {

			List<Libro> lista = new ArrayList<Libro>(100);
			while (c.next()) {
				Libro l = new Libro();
				l.setTitulo(c.getString(1));
				l.setISBN(c.getInt(2));
				l.setFecha(c.getDate(3));
				l.setPrecio(c.getBigDecimal(4));
				l.setTexto(c.getClobAsString(5));
				lista.add(l);
			}
			return lista;
		});

		JdbcWrapper.debug("********************* Libros en la biblioteca ***************************");
		for (Libro l : libros) {
			JdbcWrapper.debug(l.toString());
		}
		JdbcWrapper.debug("********************* ----------------------- ***************************");
	}
	
	public static void getNuevoLibro(int isbn, String[] libro) {
		libro[0] = "La fundación";
	}
	
	public static String getNuevoLibroFuncion(int isbn) {
		return "Los límites de la fundación";
	}
}
