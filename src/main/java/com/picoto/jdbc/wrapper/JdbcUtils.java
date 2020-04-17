package com.picoto.jdbc.wrapper;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtils {
	
	private static final int BUFFER_SIZE = 1024;

	public static String readFully(InputStream is) throws IOException {

		BufferedInputStream bis = new BufferedInputStream(is);
		ByteArrayOutputStream buf = new ByteArrayOutputStream();
		byte[] buffer = new byte[BUFFER_SIZE];
		int result = bis.read(buffer);
		while (result != -1) {
			buf.write(buffer,0, result);
			result = bis.read(buffer);
		}

		return buf.toString("UTF-8");
	}
	
	public static byte[] readFullyBinary(InputStream is) throws IOException {

		BufferedInputStream bis = new BufferedInputStream(is);
		ByteArrayOutputStream buf = new ByteArrayOutputStream();
		byte[] buffer = new byte[BUFFER_SIZE];
		int result = bis.read(buffer);
		while (result != -1) {
			buf.write(buffer,0, result);
			result = bis.read(buffer);
		}

		return buf.toByteArray();
	}

	public static List<String> extractFields(final ClassWrapper<String> namedQueryWrapper) {
		List<String> fields = new ArrayList<String>();
		String namedQueryStr = namedQueryWrapper.getValue();
		int pos;

		while ((pos = namedQueryStr.indexOf(":")) != -1) {
			int end = namedQueryStr.substring(pos).indexOf(" ");
			if (end == -1)
				end = namedQueryStr.length();
			else
				end += pos;
			fields.add(namedQueryStr.substring(pos + 1, end));
			namedQueryStr = namedQueryStr.substring(0, pos) + "?" + namedQueryStr.substring(end);
		}
		namedQueryWrapper.setValue(namedQueryStr);
		
		return fields;
	}
	
	public static String capitalize(String str) {
		return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
	}
	
}
