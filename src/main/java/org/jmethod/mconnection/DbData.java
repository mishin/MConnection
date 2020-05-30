package org.jmethod.mconnection;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbData {
    private String tableName;
    private Object id;
    private boolean done = false;
    private boolean debugFlag = true;

    private List<Object> values = new ArrayList<>();
    private List<String> fieldNames = new ArrayList<>();
    private Map<String, Integer> indexes = new HashMap<>();

    // constructor
    public DbData() {
    }

    // constructor
    public DbData(String tableName) {
        this.tableName = tableName;
    }

    // constructor
    public DbData(String tableName, Object id) {
        this.tableName = tableName;
        this.id = id;
    }

    public boolean isCorrect() {
        int v = values.size();
        return v == fieldNames.size() && v == indexes.size();
    }

    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Object getId() {
        return id;
    }
    public void setId(Object id) {
        this.id = id;
    }

    public boolean getDone() {
        return done;
    }
    public void setDone(boolean done) {
        this.done = done;
    }

    public boolean getDebugFlag() {
        return debugFlag;
    }
    public void setDebugFlag(boolean debugFlag) {
        this.debugFlag = debugFlag;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }
    public List<Object> getValues() {
        return values;
    }

    // set by fieldName
    public synchronized DbData setObject(String fieldName, Object value) {
        int index = values.size();
        values.add(value);
        indexes.put(fieldName, index);
        fieldNames.add(fieldName);
        return this;
    }

    public DbData setBoolean(String fieldName, boolean value) {
        return setObject(fieldName, value);
    }
    public DbData SetBoolean(String fieldName, Boolean value) {
        return setObject(fieldName, value);
    }

    public DbData setByte(String fieldName, byte value) {
        return setObject(fieldName, value);
    }
    public DbData SetByte(String fieldName, Byte value) {
        return setObject(fieldName, value);
    }

    public DbData setShort(String fieldName, short value) {
        return setObject(fieldName, value);
    }
    public DbData SetShort(String fieldName, Short value) {
        return setObject(fieldName, value);
    }

    public DbData setInt(String fieldName, int value) {
        return setObject(fieldName, value);
    }
    public DbData SetInt(String fieldName, Integer value) {
        return setObject(fieldName, value);
    }

    public DbData setLong(String fieldName, long value) {
        return setObject(fieldName, value);
    }
    public DbData SetLong(String fieldName, Long value) {
        return setObject(fieldName, value);
    }

    public DbData setFloat(String fieldName, float value) {
        return setObject(fieldName, value);
    }
    public DbData SetFloat(String fieldName, Float value) {
        return setObject(fieldName, value);
    }

    public DbData setDouble(String fieldName, double value) {
        return setObject(fieldName, value);
    }
    public DbData SetDouble(String fieldName, Double value) {
        return setObject(fieldName, value);
    }

    public DbData setBigDecimal(String fieldName, BigDecimal value) {
        return setObject(fieldName, value);
    }

    public DbData setString(String fieldName, String value) {
        return setObject(fieldName, value);
    }

    public DbData setSqlDate(String fieldName, java.sql.Date value) {
        return setObject(fieldName, value);
    }

    public DbData setTime(String fieldName, java.sql.Time value) {
        return setObject(fieldName, value);
    }

    public DbData setDate(String fieldName, java.util.Date value) {
        return setObject(fieldName, value);
    }

    public DbData setTimestamp(String fieldName, java.sql.Timestamp value) {
        return setObject(fieldName, value);
    }

    // get by fieldIndex
    public Object getObject(int fieldIndex) {
        return values.get(fieldIndex);
    }

    public boolean getBoolean(int fieldIndex) {
        return (Boolean) getObject(fieldIndex);
    }
    public Boolean GetBoolean(int fieldIndex) {
        return (Boolean) getObject(fieldIndex);
    }

    public byte getByte(int fieldIndex) {
        return (Byte) getObject(fieldIndex);
    }
    public Byte GetByte(int fieldIndex) {
        return (Byte) getObject(fieldIndex);
    }

    public short getShort(int fieldIndex) {
        return (Short) getObject(fieldIndex);
    }
    public Short GetShort(int fieldIndex) {
        return (Short) getObject(fieldIndex);
    }

    public int getInteger(int fieldIndex) {
        return (Integer) getObject(fieldIndex);
    }
    public Integer GetInteger(int fieldIndex) {
        return (Integer) getObject(fieldIndex);
    }

    public long getLong(int fieldIndex) {
        return (Long) getObject(fieldIndex);
    }
    public Long GetLong(int fieldIndex) {
        return (Long) getObject(fieldIndex);
    }

    public float getFloat(int fieldIndex) {
        return (Float) getObject(fieldIndex);
    }
    public Float GetFloat(int fieldIndex) {
        return (Float) getObject(fieldIndex);
    }

    public double getDouble(int fieldIndex) {
        return (Double) getObject(fieldIndex);
    }
    public Double GetDouble(int fieldIndex) {
        return (Double) getObject(fieldIndex);
    }

    public BigDecimal getDecimal(int fieldIndex) {
        return (BigDecimal) getObject(fieldIndex);
    }

    public String getString(int fieldIndex) {
        return (String) getObject(fieldIndex);
    }

    public java.sql.Date getSqlDate(int fieldIndex) {
        return (java.sql.Date) getObject(fieldIndex);
    }

    public java.sql.Time getTime(int fieldIndex) {
        return (java.sql.Time) getObject(fieldIndex);
    }

    public java.util.Date getDate(int fieldIndex) {
        return (java.util.Date) getObject(fieldIndex);
    }

    public java.sql.Timestamp getTimestamp(int fieldIndex) {
        return (java.sql.Timestamp) getObject(fieldIndex);
    }

    // get by fieldName
    public Object getObject(String fieldName) {
        int index = indexes.get(fieldName);
        return getObject(index);
    }

    public boolean getBoolean(String fieldName) {
        return (Boolean) getObject(fieldName);
    }
    public Boolean GetBoolean(String fieldName) {
        return (Boolean) getObject(fieldName);
    }

    public byte getByte(String fieldName) {
        return (Byte) getObject(fieldName);
    }
    public Byte GetByte(String fieldName) {
        return (Byte) getObject(fieldName);
    }

    public short getShort(String fieldName) {
        return (Short) getObject(fieldName);
    }
    public Short GetShort(String fieldName) {
        return (Short) getObject(fieldName);
    }

    public int getInteger(String fieldName) {
        return (Integer) getObject(fieldName);
    }
    public Integer GetInteger(String fieldName) {
        return (Integer) getObject(fieldName);
    }

    public long getLong(String fieldName) {
        return (Long) getObject(fieldName);
    }
    public Long    GetLong(String fieldName) {
        return (Long) getObject(fieldName);
    }

    public float getFloat(String fieldName) {
        return (Float) getObject(fieldName);
    }
    public Float GetFloat(String fieldName) {
        return (Float) getObject(fieldName);
    }

    public double getDouble(String fieldName) {
        return (Double) getObject(fieldName);
    }
    public Double GetDouble(String fieldName) {
        return (Double) getObject(fieldName);
    }

    public BigDecimal getDecimal(String fieldName) {
        return (BigDecimal) getObject(fieldName);
    }

    public String  getString(String fieldName) {
        return (String) getObject(fieldName);
    }

    public java.sql.Date getSqlDate(String fieldName) {
        return (java.sql.Date) getObject(fieldName);
    }

    public java.sql.Time getTime(String fieldName) {
        return (java.sql.Time) getObject(fieldName);
    }

    public java.util.Date getDate(String fieldName) {
        return (java.util.Date) getObject(fieldName);
    }

    public java.sql.Timestamp getTimestamp(String fieldName) {
        return (java.sql.Timestamp) getObject(fieldName);
    }

    // get Field Name by ind
    public String getFieldName(int fieldIndex) {
        return fieldNames.get(fieldIndex);
    }

    // toStr
    public String toStr(String delim) {
        StringBuilder sb = new StringBuilder();
        sb
                .append("tableName=")
                .append(this.tableName)
                .append("  id=")
                .append(this.id)
                .append("  done=")
                .append(this.done)
                .append( "\r\n" );

        List<String> fns = this.getFieldNames();
        if (!fns.isEmpty()) {
            sb.append("  ");
            for (int i = 0; i < fns.size(); i++) {
                if (i != 0) {
                    sb.append(delim);
                }
                sb
                        .append(this.getFieldName(i))
                        .append("=")
                        .append(this.getObject(i));
            }
        } else {
            sb.append("  Data container is EMPTY.");
        }

        return sb.toString();
    } // toStr

    public String toStr(){
        return toStr( "  " );
    }

    public static void main(String[] args) {
        long ct = System.currentTimeMillis();

        DbData dbd = new DbData("PRODUCT", 1L);
        dbd.setString("NAME", "alex-1");
        dbd.setLong("NUM", 1L);
        dbd.setBigDecimal("DEC", BigDecimal.valueOf(123456789012.12));
        dbd.setSqlDate("SDATE", new java.sql.Date(ct));
        dbd.setDate("DDATE", new java.util.Date(ct));
        dbd.setTime("OTIME", new java.sql.Time(ct));
        dbd.setTimestamp("ODATETIME", new java.sql.Timestamp(ct));

        System.out.println("dbd=" + dbd.toStr());
    }
}
