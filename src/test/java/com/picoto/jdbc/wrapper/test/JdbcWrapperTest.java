package com.picoto.jdbc.wrapper.test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.derby.tools.ij;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.picoto.jdbc.wrapper.ClassWrapper;
import com.picoto.jdbc.wrapper.JdbcWrapper;
import com.picoto.jdbc.wrapper.JdbcWrapperException;

public class JdbcWrapperTest {

	Connection con;

	@After
	public void closeConnection() {
		JdbcWrapper.close(con);
	}

	@Before
	public void getConnection() {
		try {
			String driver = "org.apache.derby.jdbc.EmbeddedDriver";
			String connectionURL = "jdbc:derby:database/testDB;create=true";
			Class.forName(driver);
			con = DriverManager.getConnection(connectionURL, "user", "pass");
			ij.runScript(con, new FileInputStream("database/create.sql"), "UTF-8", new ByteArrayOutputStream(1),
					"ISO-8859-1");
		} catch (Exception e) {
			throw new JdbcWrapperException("No hay conexión con BB.DD.");
		}
	}

	@Test
	public void llamarPA() {
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

		Assert.assertEquals("La fundación", wrapper.getValue());

	}

	@Test
	public void llamarPAFuncion() {
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
		Assert.assertEquals("Los límites de la fundación", wrapper.getValue());
	}

	@Test
	public void contarLibros() {
		JdbcWrapper<Integer> testWrap = new JdbcWrapper<>();
		testWrap.setConnection(con);
		int total = testWrap.count("select count(*) from libros");

		JdbcWrapper.debug("* Contando libros");
		JdbcWrapper.debug("Total libros: " + total);
		Assert.assertEquals(2, total);

	}

	@Test
	public void consultarLibro() {
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

		Assert.assertEquals(1, libros.size());

	}

	@Test
	public void crearLibro() {
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

	@Test
	public void actualizarLibro() {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<Libro>();
		testWrap.setConnection(con);
		int rowsUpdated = testWrap.update("update libros set precio = 25.52 where titulo = ?", ps -> {
			ps.setString(1, "El Silmarilion");
		});
		testWrap.commit(con);
		JdbcWrapper.debug("* Libro actualizado");
		Assert.assertEquals(0, rowsUpdated);
	}

	@Test
	public void borrarLibro() {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<Libro>();
		testWrap.setConnection(con);
		int rowsDeleted = testWrap.delete("delete from libros where titulo = ?", ps -> {
			ps.setString(1, "El Silmarilion");
		});
		testWrap.commit(con);
		JdbcWrapper.debug("* Libro borrado");
		Assert.assertEquals(0, rowsDeleted);

	}

	@Test
	public void consultaLibro2() {
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

		Assert.assertEquals(1, libros.size());

	}

	@Test
	public void consultarTodos() {
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

		Assert.assertEquals(2, libros.size());

	}

}
