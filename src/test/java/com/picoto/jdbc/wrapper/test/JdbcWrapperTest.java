package com.picoto.jdbc.wrapper.test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.text.ParseException;
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
import com.picoto.jdbc.wrapper.JdbcWrapperFactory;

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
			ij.runScript(getConnection(), new FileInputStream("database/create.sql"), "UTF-8",
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

	private Connection getConnection() {
		try {
			return ds.getConnection();
		} catch (Exception e) {
			throw new JdbcWrapperException("Sin conexion");
		}
	}

	@Test
	public void llamarPA() {
		final ClassWrapper<String> wrapper = new ClassWrapper<String>();
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);

		testWrap.callProcedure("EJEMPLOPA(?,?)", cs -> {
			cs.registrarEntradaInt(1, 4);
			cs.registrarSalidaString(2);
		}, cso -> {
			wrapper.setValue(cso.getString(2));
		}, false);

		testWrap.debug("* Recuperar libro desde PA:  " + wrapper.getValue());

		Assert.assertEquals("La fundacion", wrapper.getValue());

	}

	@Test
	public void llamarPAFuncion() {
		final ClassWrapper<String> wrapper = new ClassWrapper<String>();
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);

		testWrap.callFunction("EJEMPLOPAFUNC(?)", cs -> {
			cs.registrarSalidaString(1);
			cs.registrarEntradaInt(2, 4);
		}, cso -> {
			wrapper.setValue(cso.getString(1));
		});

		testWrap.debug("* Recuperar libro desde PA con format de funcion:  " + wrapper.getValue());
		Assert.assertEquals("Los limites de la fundacion", wrapper.getValue());
	}

	@Test
	public void contarLibros() {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);

		int total = testWrap.count("select count(*) from libros");

		testWrap.debug("* Contando libros");
		testWrap.debug("Total libros: " + total);
		Assert.assertEquals(2, total);

	}
	
	@Test
	public void contarLibrosFiltro() {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);

		int total = testWrap.count("select count(*) from libros where ISBN = ?", ps -> {
			ps.setInt(1, 1);
		});

		testWrap.debug("* Contando libros con filtro por isbn");
		testWrap.debug("Total libros: " + total);
		Assert.assertEquals(1, total);

	}

	@Test
	public void consultarLibro() {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);

		List<Libro> libros = testWrap.query("select titulo, isbn, fecha, precio, texto from libros where isbn = ?",
				ps -> {
					ps.setInt(1, 2);
				}, c -> {
					Libro l = new Libro();
					l.setTitulo(c.getString(1));
					l.setIsbn(c.getInt(2));
					l.setFecha(c.getDate(3));
					l.setPrecio(c.getBigDecimal(4));
					l.setTexto(c.getClobAsString(5));
					return l;
				});

		testWrap.debug("* Libros recuperados por ISBN");
		for (Libro l : libros) {
			testWrap.debug(l.toString());
		}

		Assert.assertEquals(1, libros.size());

	}

	@Test
	public void recuperarLibro() {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);

		Libro libro = testWrap.getObject("select titulo, isbn, fecha, precio, texto from libros where isbn = ?", ps -> {
			ps.setInt(1, 2);
		}, c -> {
			Libro l = new Libro();
			l.setTitulo(c.getString(1));
			l.setIsbn(c.getInt(2));
			l.setFecha(c.getDate(3));
			l.setPrecio(c.getBigDecimal(4));
			l.setTexto(c.getClobAsString(5));
			return l;
		});

		testWrap.debug("* Libro unicorecuperado por ISBN");
		testWrap.debug(libro.toString());

		Assert.assertNotNull(libro);

	}

	@Test
	public void crearLibro() {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);
		testWrap.insert("insert into libros (isbn, titulo, fecha, precio, texto) values (?, ?, ?, ?, ?)", ps -> {
			ps.setInt(1, 3);
			ps.setString(2, "El Silmarilion");
			ps.setDate(3, new Date());
			ps.setBigDecimal(4, BigDecimal.valueOf(34.56f));
			ps.setClob(5, "Vaya rollo de libro");
		});
		testWrap.debug("* Libro creado");

	}

	@Test
	public void actualizarLibro() {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), true, true);
		int rowsUpdated = testWrap.update("update libros set precio = 25.52 where titulo = ?", ps -> {
			ps.setString(1, "El Silmarilion");
		});
		testWrap.debug("* Libro actualizado");
		Assert.assertTrue(1 >= rowsUpdated);
	}

	@Test
	public void borrarLibro() {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), true, true);
		testWrap.delete("delete from libros where titulo = ?", ps -> {
			ps.setString(1, "El Silmarilion");
		});
		testWrap.debug("* Libro borrado");

	}

	@Test
	public void consultarTodos() {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);

		List<Libro> libros = testWrap.query("select titulo, isbn, fecha, precio, texto from libros", c -> {
			Libro l = new Libro();
			l.setTitulo(c.getString(1));
			l.setIsbn(c.getInt(2));
			l.setFecha(c.getDate(3));
			l.setPrecio(c.getBigDecimal(4));
			l.setTexto(c.getClobAsString(5));
			return l;
		});

		testWrap.debug("********************* Libros en la biblioteca ***************************");
		for (Libro l : libros) {
			testWrap.debug(l.toString());
		}
		testWrap.debug("********************* ----------------------- ***************************");

		Assert.assertEquals(2, libros.size());

	}

	@Test
	public void getLibro() throws ParseException {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);
		Libro libro = testWrap.getQuery(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = ? and titulo = ? and precio = ? and fecha = ?")
				.parameterInteger(1).parameterString("El señor de los anillos")
				.parameterBigDecimal(new BigDecimal("12.34")).parameterDate(new Date()).getObject(Libro.class);

		testWrap.debug("********************* Libro recuperado ***************************");
		testWrap.debug(libro.toString());
		testWrap.debug("********************* ----------------------- ***************************");

		Assert.assertNotNull(libro);

	}

	@Test
	public void getLibroConMapper() throws ParseException {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);
		Libro libro = testWrap.getQuery(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = ? and titulo = ? and precio = ? and fecha = ?")
				.parameterInteger(1).parameterString("El señor de los anillos")
				.parameterBigDecimal(new BigDecimal("12.34")).parameterDate(new Date()).getMappedObject(c -> {
					Libro l = new Libro();
					l.setTitulo(c.getString(1));
					l.setIsbn(c.getInt(2));
					l.setFecha(c.getDate(3));
					l.setPrecio(c.getBigDecimal(4));
					l.setTexto(c.getClobAsString(5));
					return l;
				});

		testWrap.debug("********************* Libro recuperado ***************************");
		testWrap.debug(libro.toString());
		testWrap.debug("********************* ----------------------- ***************************");

		Assert.assertNotNull(libro);

	}

	@Test
	public void consultaLibroEstiloCascadaSinMapper() throws ParseException {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);
		List<Libro> libros = testWrap.getQuery(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = ? and titulo = ? and precio = ? and fecha = ?")
				.parameterInteger(1).parameterString("El señor de los anillos")
				.parameterBigDecimal(new BigDecimal("12.34")).parameterDate(new Date()).executeQuery(Libro.class);

		testWrap.debug("********************* Libros consulta inline ***************************");
		for (Libro l : libros) {
			testWrap.debug(l.toString());
		}
		testWrap.debug("********************* ----------------------- ***************************");

		Assert.assertEquals(1, libros.size());

	}

	@Test
	public void consultaLibroEstiloCascadaConMapper() throws ParseException {
		JdbcWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcWrapper(Libro.class, getConnection(), false, true);

		List<Libro> libros = testWrap.getQuery(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = ? and titulo = ? and precio = ? and fecha = ?")
				.parameterInteger(1).parameterString("El señor de los anillos")
				.parameterBigDecimal(new BigDecimal("12.34")).parameterDate(new Date()).executeMappedQuery(c -> {
					Libro l = new Libro();
					l.setTitulo(c.getString(1));
					l.setIsbn(c.getInt(2));
					l.setFecha(c.getDate(3));
					l.setPrecio(c.getBigDecimal(4));
					l.setTexto(c.getClobAsString(5));
					return l;
				});

		testWrap.debug("********************* Libros consulta inline ***************************");
		for (Libro l : libros) {
			testWrap.debug(l.toString());
		}
		testWrap.debug("********************* ----------------------- ***************************");

		Assert.assertEquals(1, libros.size());

	}

}
