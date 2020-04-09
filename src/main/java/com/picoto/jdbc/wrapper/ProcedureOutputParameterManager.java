package com.picoto.jdbc.wrapper;

import java.sql.SQLException;

@FunctionalInterface
public interface ProcedureOutputParameterManager {

	public abstract void getOutputParameters(ProcedureOutputParameterGetter paramGetter) throws SQLException;

}
