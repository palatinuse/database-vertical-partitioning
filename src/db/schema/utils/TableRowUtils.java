package db.schema.utils;

import db.schema.entity.Attribute;
import db.schema.entity.Table;
import db.schema.entity.TableRow;
import db.schema.entity.TableRow.NOVALUE;
import db.schema.types.AttributeType;
import db.schema.types.AttributeType.DateAndTimeAttributeType;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Scanner;

public class TableRowUtils {

	public static double[] mapToDouble(TableRow tr) throws ParseException{
		Object[] values = tr.getValues();
		double[] doubleValues = new double[values.length];
		for(int i=0;i<values.length;i++){
			AttributeType attType = tr.getTable().attributes.get(i).type;
			if(values[i]==null)
				doubleValues[i] = 0;
			else{
				switch(attType.getId()){
				case Types.SMALLINT:
					doubleValues[i] = (Short)values[i]; break;
				case Types.BIGINT:
					doubleValues[i] = (Long)values[i]; break;
				case Types.INTEGER:
					doubleValues[i] = (Integer)values[i]; break;
				case Types.NUMERIC:
					doubleValues[i] = (Long)values[i]; break;
				case Types.REAL:
					doubleValues[i] = (Float)values[i]; break;
				case Types.DOUBLE:
					doubleValues[i] = (Double)values[i]; break;
				case Types.BOOLEAN:
					doubleValues[i] = (Boolean)values[i]==true?1:0; break;
				case Types.CHAR: 
					doubleValues[i] = stringToLong((String)values[i]); break;
				case Types.VARCHAR: 
					doubleValues[i] = stringToLong((String)values[i]); break;
				case Types.DATE: 
					DateFormat dateParser = ((DateAndTimeAttributeType)attType).getDateFormat();
					doubleValues[i] = dateParser.parse((String)values[i]).getTime();
					break;
				case Types.TIMESTAMP:
					dateParser = ((DateAndTimeAttributeType)attType).getDateFormat();
					doubleValues[i] = dateParser.parse((String)values[i]).getTime();
					break;
				case Types.BLOB:
					throw new UnsupportedOperationException("SQL type "+attType.getId()+" not supported!");
				default: throw new UnsupportedOperationException("SQL type "+attType.getId()+" not supported!");
				}
			}
		}
		return doubleValues;
	}
	
	private static long stringToLong(String s){
		long value = 0;
		for(int i=0;i<s.length();i++){
			int ascii = (int)(s.charAt(i)) - 32;
			if(ascii < 0 || ascii > 94)
				throw new RuntimeException("Non-printable character found: "+s.charAt(i));
			value += Math.pow(2, i)*ascii;
			//value += Math.pow(95, i)*ascii;
		}
		return value;
	}
	
	public static void setValues(TableRow tr, PreparedStatement ps) throws SQLException, ParseException{
		Object[] values = tr.getValues();
		int arrayIdx = 1;
		for(int i=0;i<values.length;i++){
			if(values[i] instanceof NOVALUE)
				continue;
			
			AttributeType attType = tr.getTable().attributes.get(i).type;
			
			if(values[i]==null){
				ps.setNull(arrayIdx++, attType.getId());
			}
			else{
				switch(attType.getId()){
				case Types.SMALLINT:
					ps.setShort(arrayIdx++, (Short)values[i]); break;
				case Types.BIGINT:
					ps.setLong(arrayIdx++, (Long)values[i]); break;
				case Types.INTEGER:
					ps.setInt(arrayIdx++, (Integer)values[i]); break;
				case Types.NUMERIC:
					ps.setLong(arrayIdx++, (Long)values[i]); break;
				case Types.REAL:
					ps.setFloat(arrayIdx++, (Float)values[i]); break;
				case Types.DOUBLE:
					ps.setDouble(arrayIdx++, (Double)values[i]); break;
				case Types.BOOLEAN:
					ps.setBoolean(arrayIdx++, (Boolean)values[i]); break;
				case Types.CHAR: 
					ps.setString(arrayIdx++, (String)values[i]); break;
				case Types.VARCHAR: 
					ps.setString(arrayIdx++, (String)values[i]); break;
				case Types.DATE: 
					DateFormat dateParser = ((DateAndTimeAttributeType)attType).getDateFormat();
					long time = dateParser.parse((String)values[i]).getTime();
					ps.setDate(arrayIdx++, new Date(time));
					break;
				case Types.TIMESTAMP:
					dateParser = ((DateAndTimeAttributeType)attType).getDateFormat();
					time = dateParser.parse((String)values[i]).getTime();
					ps.setTimestamp(arrayIdx++, new Timestamp(time));
					break;
				case Types.BLOB:
					//ps.setNull(arrayIdx++, Types.BLOB);break;
					ps.setBytes(arrayIdx++, (byte[])values[i]); break;
				default: throw new UnsupportedOperationException("SQL type "+attType.getId()+" not supported!");
				}
			}
		}
	}
	
	public static TableRow partialTableRow(TableRow row, Table partitionTable, int[] attributes){
		TableRow partialRow = new TableRow(partitionTable);
		Object[] values = row.getValues();
		for(int attribute: attributes)
			partialRow.add(values[attribute]);
		return partialRow;
	}
	
	
	
	/**
	 * Iterates over data files in TPCH-like format.
	 * @author alekh
	 *
	 */
	public static class TableRowIterator implements Iterator<TableRow>{

		private Table t;
		private Scanner rowScanner; 
		
		public TableRowIterator(Table t, String dataFileName, String delimiter){
			this.t = t;
			try {
				rowScanner = new Scanner(new FileReader(dataFileName));
				rowScanner.useLocale(Locale.US);
				rowScanner.useDelimiter(delimiter);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		public boolean hasNext() {
			if(rowScanner.hasNext())
				return true;
			else{
				rowScanner.close();
				return false;
			}
		}

		public TableRow next() {
			TableRow tr = new TableRow(t);
			for(Attribute a: t.attributes){
				tr.add(AttributeTypeUtils.scanAttribute(a.type, rowScanner));
			}
			rowScanner.nextLine();
			return tr;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
}
