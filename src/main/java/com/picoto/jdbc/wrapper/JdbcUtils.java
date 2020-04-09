package com.picoto.jdbc.wrapper;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

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

}
