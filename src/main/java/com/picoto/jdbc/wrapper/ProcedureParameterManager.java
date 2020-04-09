package com.picoto.jdbc.wrapper;

import java.sql.SQLException;

@FunctionalInterface
public interface ProcedureParameterManager {



	void configureParameters(ProcedureParameterSetter paramSetter) throws SQLException;

}
