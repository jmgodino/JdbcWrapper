package com.picoto.jdbc.wrapper.test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.derby.tools.ij;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.picoto.jdbc.wrapper.ClassWrapper;
import com.picoto.jdbc.wrapper.JdbcWrapper;
import com.picoto.jdbc.wrapper.JdbcWrapperException;

/*import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;*/

public class JdbcWrapperTest {

	DataSource ds;

	@Before
	public void getDataSource() {

		try {

			EmbeddedDataSource dsDerby = new EmbeddedDataSource();
			dsDerby.setDatabaseName("database/testDB");
			dsDerby.setCreateDatabase("create");
			ds = dsDerby;
			ij.runScript(ds.getConnection(), new FileInputStream("database/create.sql"), "UTF-8",
					new ByteArrayOutputStream(1), "ISO-8859-1");

			/*
			 * PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
			 * pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
			 * pds.setURL("jdbc:oracle:thin:@//192.168.1.43:1521/orcl");
			 * pds.setUser("dummy"); pds.setPassword("1234"); ds = pds;
			 */

		} catch (Exception e) {
			throw new JdbcWrapperException("No hay conexión con BB.DD.");
		}
	}

	@Test
	public void llamarPA() {
		final ClassWrapper<String> wrapper = new ClassWrapper<String>();
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setDataSource(ds);
		testWrap.callProcedure("EJEMPLOPA(?,?)", cs -> {
			cs.registrarEntradaInt(1, 4);
			cs.registrarSalidaString(2);
		}, cso -> {
			wrapper.setValue(cso.getString(2));
		}, false);

		JdbcWrapper.debug("* Recuperar libro desde PA:  " + wrapper.getValue());

		Assert.assertEquals("La fundacion", wrapper.getValue());

	}

	@Test
	public void llamarPAFuncion() {
		final ClassWrapper<String> wrapper = new ClassWrapper<String>();
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setDataSource(ds);
		testWrap.callProcedure("EJEMPLOPAFUNC(?)", cs -> {
			cs.registrarSalidaString(1);
			cs.registrarEntradaInt(2, 4);
		}, cso -> {
			wrapper.setValue(cso.getString(1));
		}, true);

		JdbcWrapper.debug("* Recuperar libro desde PA con format de funcion:  " + wrapper.getValue());
		Assert.assertEquals("Los limites de la fundacion", wrapper.getValue());
	}

	@Test
	public void contarLibros() {
		JdbcWrapper<Integer> testWrap = new JdbcWrapper<>();
		testWrap.setDataSource(ds);
		int total = testWrap.count("select count(*) from libros");

		JdbcWrapper.debug("* Contando libros");
		JdbcWrapper.debug("Total libros: " + total);
		Assert.assertEquals(2, total);

	}

	@Test
	public void consultarLibro() {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setDataSource(ds);
		List<Libro> libros = testWrap.query("select titulo, isbn, fecha, precio, texto from libros where isbn = ?",
				ps -> {
					ps.setInt(1, 2);
				}, c -> {

					List<Libro> lista = new ArrayList<Libro>(100);
					while (c.next()) {
						Libro l = new Libro();
						l.setTitulo(c.getString(1));
						l.setIsbn(c.getInt(2));
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
		testWrap.setDataSource(ds, true);
		testWrap.insert("insert into libros (isbn, titulo, fecha, precio, texto) values (?, ?, ?, ?, ?)", ps -> {
			ps.setInt(1, 3);
			ps.setString(2, "El Silmarilion");
			ps.setDate(3, new Date());
			ps.setBigDecimal(4, BigDecimal.valueOf(34.56f));
			ps.setClob(5, "Vaya rollo de libro");
		});
		JdbcWrapper.debug("* Libro creado");

	}

	@Test
	public void actualizarLibro() {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<Libro>();
		testWrap.setDataSource(ds, true);
		int rowsUpdated = testWrap.update("update libros set precio = 25.52 where titulo = ?", ps -> {
			ps.setString(1, "El Silmarilion");
		});
		JdbcWrapper.debug("* Libro actualizado");
		Assert.assertTrue(1 >= rowsUpdated);
	}

	@Test
	public void borrarLibro() {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<Libro>();
		testWrap.setDataSource(ds, true);
		testWrap.delete("delete from libros where titulo = ?", ps -> {
			ps.setString(1, "El Silmarilion");
		});
		JdbcWrapper.debug("* Libro borrado");

	}

	@Test
	public void consultaLibro2() {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setDataSource(ds);
		List<Libro> libros = testWrap.namedQuery(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = :isbn and titulo = :titulo", np -> {
					np.setInt("isbn", 1);
					np.setString("titulo", "El señor de los anillos");
				}, c -> {

					List<Libro> lista = new ArrayList<Libro>(100);
					while (c.next()) {
						Libro l = new Libro();
						l.setTitulo(c.getString(1));
						l.setIsbn(c.getInt(2));
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
		testWrap.setDataSource(ds);
		List<Libro> libros = testWrap.query("select titulo, isbn, fecha, precio, texto from libros", c -> {

			List<Libro> lista = new ArrayList<Libro>(100);
			while (c.next()) {
				Libro l = new Libro();
				l.setTitulo(c.getString(1));
				l.setIsbn(c.getInt(2));
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

	@Test
	public void consultaLibroEstiloCascada() throws ParseException {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setDataSource(ds);
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		Date fecha = sdf.parse("12/04/2020");
		List<Libro> libros = testWrap.getQuery(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = ? and titulo = ? and precio = ? and fecha = ?")
				.parameterInteger(1).parameterString("El señor de los anillos")
				.parameterBigDecimal(new BigDecimal("12.34")).parameterDate(fecha).executeQuery(Libro.class);

		JdbcWrapper.debug("********************* Libros consulta inline ***************************");
		for (Libro l : libros) {
			JdbcWrapper.debug(l.toString());
		}
		JdbcWrapper.debug("********************* ----------------------- ***************************");

		Assert.assertEquals(1, libros.size());

	}

	@Test
	public void consultaLibroEstiloCascada2() throws ParseException {
		JdbcWrapper<Libro> testWrap = new JdbcWrapper<>();
		testWrap.setDataSource(ds);
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		Date fecha = sdf.parse("12/04/2020");
		List<Libro> libros = testWrap.getQuery(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = ? and titulo = ? and precio = ? and fecha = ?")
				.parameterInteger(1).parameterString("El señor de los anillos")
				.parameterBigDecimal(new BigDecimal("12.34")).parameterDate(fecha).executeMapperQuery(c -> {
					List<Libro> lista = new ArrayList<Libro>(100);
					while (c.next()) {
						Libro l = new Libro();
						l.setTitulo(c.getString(1));
						l.setIsbn(c.getInt(2));
						l.setFecha(c.getDate(3));
						l.setPrecio(c.getBigDecimal(4));
						l.setTexto(c.getClobAsString(5));
						lista.add(l);
					}
					return lista;
				});

		JdbcWrapper.debug("********************* Libros consulta inline ***************************");
		for (Libro l : libros) {
			JdbcWrapper.debug(l.toString());
		}
		JdbcWrapper.debug("********************* ----------------------- ***************************");

		Assert.assertEquals(1, libros.size());

	}

}
