package org.jmethod.mconnection;

import javax.sound.midi.InvalidMidiDataException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {

    public static final String DEFAULT_DATE_PATTERN = "dd.MM.yyyy";
    public static final String SQL_DATE_PATTERN = "yyyy-MM-dd";

    public static void exceptionPoint(String mes) throws Exception {
        throw new Exception(mes);
    }

    public static String objToSqlStr( Object obj ){
        if (obj == null) {
            return "null";
        } else if (obj instanceof String){
            return "'" + obj + "'";
        } else if (obj instanceof Date){
            return Utils.dateToSqlString(Utils.objectToDate(obj));
        } else if (obj instanceof java.sql.Date){
            return Utils.dateToSqlString(sqlDateToDate((java.sql.Date) obj));
//        } else if ( obj instanceof java.sql.Time ){
//            return Utils.getSQLTimeString( Utils.objectToTime( obj ) );
        } else if (obj instanceof Boolean){
            return ((Boolean) obj).toString().toUpperCase();
//        } else if ( obj instanceof Double ){
//            return Utils.doubleToFreeStr( Utils.objectToDouble( obj ) );
        } else if (obj instanceof Long){
            return Long.toString((Long) obj);
        } else if (obj instanceof Integer){
            return Integer.toString((Integer) obj);
        } else if (obj instanceof Short){
            return Short.toString((Short) obj);
        } else if (obj instanceof Byte){
            return Byte.toString((Byte) obj);
        } // if
        return "" + obj;
    } // objToSqlStr

    public static java.util.Date objectToDate(Object ob) {
        if (ob == null) {
            return null;
        } else if (ob.getClass() == Date.class) {
            return (java.util.Date) (ob);
        } else if (ob instanceof java.sql.Date) {
            return new Date(((Date) ob).getTime());
        } else {
            //Utils.outln("Utils.objectToDate: Illegal argument: ob=" + ob + "; ob.getClass()=" + ob.getClass());
            // Чтобы было видно, где неправильно
            try {
//                String str = null;
//                str.length();
                exceptionPoint(
                        "Utils.objectToDate: Illegal argument: ob=" + ob + "; ob.getClass()=" + ob.getClass()
                );
            } catch (Exception ex) {
                ex.printStackTrace();
            } // try
            //^^Чтобы было видно, где неправильно
            return null;
        }
    }

    public static java.sql.Date dateToSqlDate(Date date) {
        return date == null ? null : new java.sql.Date(date.getTime());
    }

    public static Date sqlDateToDate(java.sql.Date sqlDate) {
        return sqlDate == null ? null : new Date(sqlDate.getTime());
    }

    public static String dateToString(Date date, String pattern) {
        pattern = pattern == null ? DEFAULT_DATE_PATTERN : pattern;
        DateFormat df = new SimpleDateFormat(pattern);
        return df.format(date);
    }

    public static String dateToString(Date date) {
        return dateToString(date, null);
    }

    public static String dateToSqlString(Date date) {
        return dateToString(date, SQL_DATE_PATTERN);
    }

    public static void outln(Object obj) {
        System.out.println(obj);
    }

    public static void out(Object obj) {
        System.out.print(obj);
    }
}
