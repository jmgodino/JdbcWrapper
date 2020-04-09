package com.picoto.jdbc.wrapper.test;

import java.math.BigDecimal;
import java.util.Date;

public class Libro {
	private String titulo;
	private int ISBN;
	private BigDecimal precio;
	private Date fecha;

	public BigDecimal getPrecio() {
		return precio;
	}

	public void setPrecio(BigDecimal precio) {
		this.precio = precio;
	}

	public Date getFecha() {
		return fecha;
	}

	public void setFecha(Date fecha) {
		this.fecha = fecha;
	}

	public String getTexto() {
		return texto;
	}

	public void setTexto(String texto) {
		this.texto = texto;
	}

	private String texto;

	public String getTitulo() {
		return titulo;
	}

	public void setTitulo(String titulo) {
		this.titulo = titulo;
	}

	public int getISBN() {
		return ISBN;
	}

	public void setISBN(int iSBN) {
		ISBN = iSBN;
	}

	@Override
	public String toString() {
		return "Libro [titulo=" + titulo + ", ISBN=" + ISBN + ", precio=" + precio + ", fecha=" + fecha + ", texto="
				+ texto + "]";
	}

	public static void getNuevoLibro(int isbn, String[] libro) {
		libro[0] = "La fundación";
	}
	
	public static String getNuevoLibroFuncion(int isbn) {
		return "Los límites de la fundación";
	}

}
