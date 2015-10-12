package db.schema.utils;

import db.schema.types.AttributeType;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Scanner;

public class AttributeTypeUtils {

	public static void copyData(AttributeType type, int idxSrc, ResultSet src, int idxDst, ResultSet dst) throws SQLException{
		switch(type.getId()){
		case Types.CHAR:		dst.updateString(idxDst, src.getString(idxSrc).trim()); break;
		case Types.VARCHAR:		dst.updateString(idxDst, src.getString(idxSrc).trim()); break;
		case Types.SMALLINT:	dst.updateShort(idxDst, src.getShort(idxSrc)); break;
		case Types.BIGINT:		dst.updateLong(idxDst, src.getLong(idxSrc)); break;
		case Types.INTEGER:		dst.updateInt(idxDst, src.getInt(idxSrc)); break;
		case Types.NUMERIC:		dst.updateLong(idxDst, src.getLong(idxSrc)); break;
		case Types.REAL:		dst.updateFloat(idxDst, src.getFloat(idxSrc)); break;
		case Types.DOUBLE:		dst.updateDouble(idxDst, src.getDouble(idxSrc)); break;
		case Types.DATE:		dst.updateDate(idxDst, src.getDate(idxSrc)); break;
		case Types.TIMESTAMP:	dst.updateTimestamp(idxDst, src.getTimestamp(idxSrc)); break;
		case Types.BOOLEAN:		dst.updateBoolean(idxDst, src.getBoolean(idxSrc)); break;
		case Types.BLOB:		dst.updateBytes(idxDst, src.getBytes(idxSrc)); break;
		}
	}

	public static Object scanAttribute(AttributeType type, Scanner rowScanner) {
		switch(type.getId()){
		case Types.CHAR:		return rowScanner.next();
		case Types.VARCHAR:		return rowScanner.next();
		case Types.SMALLINT:	return rowScanner.nextShort();
		case Types.BIGINT:		return rowScanner.nextLong();
		case Types.INTEGER:		return rowScanner.nextInt();
		case Types.NUMERIC:		return rowScanner.nextLong();
		case Types.REAL:		return rowScanner.nextFloat();
		case Types.DOUBLE:		return rowScanner.nextDouble();
		case Types.DATE:		return rowScanner.next();
		case Types.TIMESTAMP:	return rowScanner.next();
		case Types.BOOLEAN:		return rowScanner.nextBoolean();
		case Types.BLOB:		return rowScanner.next();
		}
		
		return null;
	}

	public static void copyAttributeBytesToBuffer(AttributeType type, Scanner rowScanner, ByteBuffer buffer) {
		String data;
		switch(type.getId()){
		case Types.CHAR:		data = (String)(rowScanner.next());
								buffer.putShort((short)(data.length()));
								buffer.put(data.getBytes());break;
		case Types.VARCHAR:		data = (String)(rowScanner.next());
								buffer.putShort((short)(data.length()));
								buffer.put(data.getBytes());break;
		case Types.SMALLINT:	buffer.putShort(rowScanner.nextShort());break;
		case Types.BIGINT:		buffer.putLong(rowScanner.nextLong());break;
		case Types.INTEGER:		buffer.putInt(rowScanner.nextInt());break;
		case Types.NUMERIC:		buffer.putLong(rowScanner.nextLong());break;
		case Types.REAL:		buffer.putFloat(rowScanner.nextFloat());break;
		case Types.DOUBLE:		buffer.putDouble(rowScanner.nextDouble());break;
		case Types.DATE:		data = rowScanner.next();
								buffer.putShort((short)(data.length()));
								buffer.put(data.getBytes());break;
		case Types.TIMESTAMP:	data = rowScanner.next();
								buffer.putShort((short)(data.length()));
								buffer.put(data.getBytes());break;
		case Types.BOOLEAN:		buffer.putChar((char)(rowScanner.nextBoolean()?1:0));break;
		case Types.BLOB:		byte[] byteData = rowScanner.next().getBytes();
								buffer.putInt(byteData.length);
								buffer.put(byteData);break;
		}
	}

	public static void addAttributeBytesToBuffer(AttributeType type, ResultSet rs, int index, ByteBuffer buffer) throws SQLException{
		String data;
		switch(type.getId()){
		case Types.CHAR:		data = (String)(rs.getString(index));
								buffer.putShort((short)(data.length()));
								buffer.put(data.getBytes());break;
		case Types.VARCHAR:		data = (String)(rs.getString(index));
								buffer.putShort((short)(data.length()));
								buffer.put(data.getBytes());break;
		case Types.SMALLINT:	buffer.putShort(rs.getShort(index));break;
		case Types.BIGINT:		buffer.putLong(rs.getLong(index));break;
		case Types.INTEGER:		buffer.putInt(rs.getInt(index));break;
		case Types.NUMERIC:		buffer.putLong(rs.getLong(index));break;
		case Types.REAL:		buffer.putFloat(rs.getFloat(index));break;
		case Types.DOUBLE:		buffer.putDouble(rs.getDouble(index));break;
		case Types.DATE:		data = rs.getDate(index).toString();
								buffer.putShort((short)(data.length()));
								buffer.put(data.getBytes());break;
		case Types.TIMESTAMP:	data = rs.getTimestamp(index).toString();
								buffer.putShort((short)(data.length()));
								buffer.put(data.getBytes());break;
		case Types.BOOLEAN:		buffer.putChar((char)(rs.getBoolean(index)?1:0));break;
		case Types.BLOB:		byte[] byteData = rs.getBytes(index);
								buffer.putInt(byteData.length);
								buffer.put(byteData);break;
		}
	}

	public static Object readAttributeBytesFromBuffer(AttributeType type, ByteBuffer buffer){
		byte[] data;
		switch(type.getId()){
		case Types.CHAR:		data = new byte[buffer.getShort()];
								buffer.get(data);
								return new String(data);
		case Types.VARCHAR:		data = new byte[buffer.getShort()];
								buffer.get(data);
								return new String(data);
		case Types.SMALLINT:	return buffer.getShort();
		case Types.BIGINT:		return buffer.getLong();
		case Types.INTEGER:		return buffer.getInt();
		case Types.NUMERIC:		return buffer.getLong();
		case Types.REAL:		return buffer.getFloat();
		case Types.DOUBLE:		return buffer.getDouble();
		case Types.DATE:		data = new byte[buffer.getShort()];
								buffer.get(data);
								return new String(data);
		case Types.TIMESTAMP:	data = new byte[buffer.getShort()];
								buffer.get(data);
								return new String(data);
		case Types.BOOLEAN:		return buffer.getChar()==1?true:false;
		case Types.BLOB:		data = new byte[buffer.getInt()];
								buffer.get(data);
								return data;
		}
		return null;
	}

	public static Object castAttributeValue(AttributeType type, String value) {
		switch(type.getId()){
		case Types.CHAR:		return value;
		case Types.VARCHAR:		return value;
		case Types.SMALLINT:	return Short.parseShort(value);
		case Types.BIGINT:		return Long.parseLong(value);
		case Types.INTEGER:		return Integer.parseInt(value);
		case Types.NUMERIC:		return Long.parseLong(value);
		case Types.REAL:		return Float.parseFloat(value);
		case Types.DOUBLE:		return Double.parseDouble(value);
		case Types.DATE:		return java.sql.Date.valueOf(value);
		case Types.TIMESTAMP:	return value;
		case Types.BOOLEAN:		return Boolean.parseBoolean(value);
		case Types.BLOB:		return value;
		}
		
		return null;
	}

}
