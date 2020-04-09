package com.picoto.jdbc.wrapper;

public class ClassWrapper<T> {

	private T value;

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

}
