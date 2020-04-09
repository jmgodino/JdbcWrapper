package com.picoto.jdbc.wrapper;

@FunctionalInterface
public interface ParameterManager {

	void configureParameters(ParameterSetter paramSetter);
}
