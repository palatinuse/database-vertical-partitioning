package db.schema.entity;

import db.schema.types.AttributeType;

/**
 * 
 * Define a postgreSQL attribute here
 *  
 * @author alekh
 *
 */
public class Attribute {

	public int id;	// attribute index within the table
	public String name;
	public AttributeType type;
	
	public boolean notNull = false;
	public boolean unique = false;
	public boolean primaryKey = false;
	public Table references = null;
	
	public Attribute(String name, AttributeType type){
		this.name = name;
		this.type = type;
	}
	
	public Attribute(int id, String name, AttributeType type){
		this.id = id;
		this.name = name;
		this.type = type;
	}
	
	public String toString(){
		String attrStr = name+" "+type.toString();
//		if(notNull)
//			attrStr += " NOT NULL";
//		if(unique)
//			attrStr += " UNIQUE";
//		if(primaryKey)
//			attrStr += " PRIMARY KEY";
//		if(references!=null)
//			attrStr += " REFERENCES "+references.name;
		
		return attrStr;
	}
	
	public boolean equals(Object obj){
		return this.name.equals(((Attribute)obj).name);
	}
}
