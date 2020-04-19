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

import com.picoto.jdbc.wrapper.JdbcNamedWrapper;
import com.picoto.jdbc.wrapper.JdbcWrapperException;
import com.picoto.jdbc.wrapper.JdbcWrapperFactory;

/*import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;*/

public class JdbcNamedWrapperTest {

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
	public void crearLibro() {
		JdbcNamedWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcNamedWrapper(Libro.class, getConnection(), false, true);
		testWrap.insert("insert into libros (isbn, titulo, fecha, precio, texto) values (:isbn, :titulo, :fecha, :precio, :texto)", ps -> {
			ps.setInt("isbn", 3);
			ps.setString("titulo", "El Silmarilion");
			ps.setDate("fecha", new Date());
			ps.setBigDecimal("precio", BigDecimal.valueOf(34.56f));
			ps.setClob("texto", "Vaya rollo de libro");
		});
		testWrap.debug("* Libro creado");

	}

	@Test
	public void actualizarLibro() {
		JdbcNamedWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcNamedWrapper(Libro.class, getConnection(), true, true);
		int rowsUpdated = testWrap.update("update libros set precio = 25.52 where titulo = :titulo", ps -> {
			ps.setString("titulo", "El Silmarilion");
		});
		testWrap.debug("* Libro actualizado");
		Assert.assertTrue(1 >= rowsUpdated);
	}

	@Test
	public void borrarLibro() {
		JdbcNamedWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcNamedWrapper(Libro.class, getConnection(), true, true);
		testWrap.delete("delete from libros where titulo = :titulo", ps -> {
			ps.setString("titulo", "El Silmarilion");
		});
		testWrap.debug("* Libro borrado");

	}

	@Test
	public void consultaLibro() {
		JdbcNamedWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcNamedWrapper(Libro.class, getConnection(), false,
				true);

		List<Libro> libros = testWrap.query(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = :isbn and titulo = :titulo", np -> {
					np.setInt("isbn", 1);
					np.setString("titulo", "El señor de los anillos");
				}, c -> {
					Libro l = new Libro();
					l.setTitulo(c.getString(1));
					l.setIsbn(c.getInt(2));
					l.setFecha(c.getDate(3));
					l.setPrecio(c.getBigDecimal(4));
					l.setTexto(c.getClobAsString(5));
					return l;
				});

		testWrap.debug("* Libros recuperados por ISBN y título");
		for (Libro l : libros) {
			testWrap.debug(l.toString());
		}

		Assert.assertEquals(1, libros.size());

	}

	@Test
	public void consultaLibroEstiloCascadaSinMapper() throws ParseException {
		JdbcNamedWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcNamedWrapper(Libro.class, getConnection(), false,
				true);
		List<Libro> libros = testWrap.getQuery(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = :isbn and titulo = :titulo and precio = :precio and fecha = :fecha")
				.parameterInteger("isbn", 1).parameterString("titulo", "El señor de los anillos")
				.parameterBigDecimal("precio", new BigDecimal("12.34")).parameterDate("fecha", new Date())
				.executeQuery(Libro.class);

		testWrap.debug("********************* Libros consulta inline named no mapper ***************************");
		for (Libro l : libros) {
			testWrap.debug(l.toString());
		}
		testWrap.debug("********************* ----------------------- ***************************");

		Assert.assertEquals(1, libros.size());

	}

	@Test
	public void consultaLibroEstiloCascadaConMapper() throws ParseException {
		JdbcNamedWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcNamedWrapper(Libro.class, getConnection(), false,
				true);
		List<Libro> libros = testWrap.getQuery(
				"select titulo, isbn, fecha, precio, texto from libros where ISBN = :isbn and titulo = :titulo and precio = :precio and fecha = :fecha")
				.parameterInteger("isbn", 1).parameterString("titulo", "El señor de los anillos")
				.parameterBigDecimal("precio", new BigDecimal("12.34")).parameterDate("fecha", new Date())
				.executeMappedQuery(c -> {
					Libro l = new Libro();
					l.setTitulo(c.getString(1));
					l.setIsbn(c.getInt(2));
					l.setFecha(c.getDate(3));
					l.setPrecio(c.getBigDecimal(4));
					l.setTexto(c.getClobAsString(5));
					return l;
				});

		testWrap.debug("********************* Libros consulta inline nanmed and mapper ***************************");
		for (Libro l : libros) {
			testWrap.debug(l.toString());
		}
		testWrap.debug("********************* ----------------------- ***************************");

		Assert.assertEquals(1, libros.size());

	}

	@Test
	public void consultaLibroEstiloCascadaPSinMapperTotal() throws ParseException {
		JdbcNamedWrapper<Libro> testWrap = JdbcWrapperFactory.getJdbcNamedWrapper(Libro.class, getConnection(), false,
				true);
		List<Libro> libros = testWrap.getQuery("select titulo, isbn, fecha, precio, texto from libros")
				.executeQuery(Libro.class);

		testWrap.debug("********************* Libros consulta inline named no mapper all ***************************");
		for (Libro l : libros) {
			testWrap.debug(l.toString());
		}
		testWrap.debug("********************* ----------------------- ***************************");

		Assert.assertEquals(2, libros.size());

	}

}
