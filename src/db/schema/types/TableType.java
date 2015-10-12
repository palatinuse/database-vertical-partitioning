package db.schema.types;

/**
 * 
 * Define different types of tables here
 * 
 * @author alekh
 *
 */
public class TableType {

	public static enum ID {TABLE,CTABLE,STREAMTABLE};
	
	private String tableTypeString;
	private ID id;
	
	public TableType(String tableTypeString, ID id){
		this.tableTypeString = tableTypeString;
		this.id = id;
	}
	
	public String toString(){
		return tableTypeString;
	}
	
	public ID getId(){
		return id;
	}
	
	public static TableType Default(){
		return new TableType("TABLE", ID.TABLE);
	}
	
	public static TableType Stream(){
		return new TableType("STREAMTABLE", ID.STREAMTABLE);
	}
	
	public static TableType ColumnGrouped(){
		return new TableType("CTABLE", ID.CTABLE);
	}
    
    public boolean equals(TableType other) {
        return id.equals(other.getId());
    }
}
