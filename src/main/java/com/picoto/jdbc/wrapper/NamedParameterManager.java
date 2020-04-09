package com.picoto.jdbc.wrapper;

@FunctionalInterface
public interface NamedParameterManager {

	void configureParameters(NamedParameterSetter setter);

}
