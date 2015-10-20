package core.config;

import db.schema.types.TableType;

public class DataConfig {

	public static String tpchDataFileDir = ".";
	public static String ssbDataFileDir = null;
	public static String sdsssDataFileDir = null;
	
	public static double scaleFactor = 0.1;
	
	public static TableType tableType = TableType.ColumnGrouped();

	public static String delimiter = "\\|";
}
