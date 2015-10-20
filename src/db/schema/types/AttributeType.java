package db.schema.types;

import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Define different types of attributes here
 *
 * @author alekh
 */
public class AttributeType {

    public enum ID {CHAR, VARCHAR, TEXT, SMALLINT, BIGINT, INTEGER, NUMERIC, REAL, DOUBLE, DATE, TIMESTAMP, BOOLEAN, BYTEA}

    private int attribueTypeId;
    private String attributeTypeString;
    private int size;

    public AttributeType(int attributeTypeId, String attributeTypeString, int size) {
        this.attribueTypeId = attributeTypeId;
        this.attributeTypeString = attributeTypeString;
        this.size = size;
    }

    public String toString() {
        return attributeTypeString;
    }

    public int getId() {
        return this.attribueTypeId;
    }

    public int getSize() {
        return this.size;
    }

    public static class DateAndTimeAttributeType extends AttributeType {
        DateFormat dateFormat;

        public DateAndTimeAttributeType(int attributeTypeId, String attributeTypeString, String format) {
            super(attributeTypeId, attributeTypeString, format.length());
            this.dateFormat = new SimpleDateFormat(format);
        }

        public DateFormat getDateFormat() {
            return dateFormat;
        }
    }

    public static AttributeType Character(int n) {
        return new AttributeType(Types.CHAR, "CHAR(" + n + ")", n);
    }

    public static AttributeType CharacterVarying(int n) {
        return new AttributeType(Types.VARCHAR, "VARCHAR(" + n + ")", n);
    }

    public static AttributeType Text() {
        return new AttributeType(Types.VARCHAR, "TEXT", 0);        // cannot determine the size of text field
    }

    public static AttributeType SmallInt() {
        return new AttributeType(Types.SMALLINT, "SMALLINT", 2);
    }

    public static AttributeType BigInt() {
        return new AttributeType(Types.BIGINT, "BIGINT", 8);
    }

    public static AttributeType Integer() {
        return new AttributeType(Types.INTEGER, "INTEGER", 4);
    }

    public static AttributeType Numeric(int p, int s) {
        return new AttributeType(Types.NUMERIC, "NUMERIC(" + p + "," + s + ")", 11 + p / 2);
    }

    public static AttributeType Real() {
        return new AttributeType(Types.REAL, "REAL", 4);
    }

    public static AttributeType Double() {
        return new AttributeType(Types.DOUBLE, "DOUBLE PRECISION", 8);
    }

    public static AttributeType Date(String format) {
        return new DateAndTimeAttributeType(Types.DATE, "DATE", format);
    }

    public static AttributeType Timestamp(String format) {
        return new DateAndTimeAttributeType(Types.TIMESTAMP, "TIMESTAMP", format);
    }

    public static AttributeType Boolean() {
        return new AttributeType(Types.BOOLEAN, "BOOLEAN", 1);
    }

    public static AttributeType Blob() {
        return new AttributeType(Types.BLOB, "BYTEA", 0);        // cannot determine the size of bytea field
    }
}
