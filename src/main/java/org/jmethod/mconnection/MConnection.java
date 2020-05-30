package org.jmethod.mconnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class MConnection {
    private static final String ID = "ID";
    private static final String TABLES = "TABLES";
    private static final String DEFAULT = "{DEFAULT}";
    private static final String DELIM = ",";

    private static final String LIMIT = "LIMIT";
    private static final String FETCH_FIRST = "FETCH FIRST";
    private static final String ROWS = "ROWS";
    private static final String ROWNUM = "ROWNUM";
    private static final String SELECT_FIRST = "SELECT FIRST";

    private static final String IS_SEE__PREFIX = "java.sql.SQLSyntaxErrorException see=";
    private static final String IS_REL_NOT_EXIST__PREFIX = "relation not exist e=";

    private Connection connection;

    // For DataSource MODE
    private DataSource dataSource;

    // For Driver MODE
    public String driver;
    public String url;
    public String login;
    public String password;

    // ID name config
    private Map<String, String> idNames = null;
    // SEQUENCES config
    private Map<String, String> sequences = null;
    // LIMIT TYPE config
    public String limitTyp = LIMIT;

    private String CRSF = null;

    // TODO Hashtable
    private Hashtable metaDataTable_Cash = new Hashtable();

    private static class ActivateResult {
        public boolean activated = false;
        public boolean done = true;
    }

    // createMConnection

    /**
     * режим драйвера,
     * деражится пока живет приложение
     * для десктопных приложений
     * @param driver
     * @param url
     * @param login
     * @param password
     * @param limitTyp
     * @param idNames
     * @param sequences
     * @return
     */
    public static MConnection createMConnection(
        String driver,
        String url,
        String login,
        String password,
        String limitTyp,
        Map<String, String> idNames,
        Map<String, String> sequences
    ){
        MConnection mc = new MConnection();
        mc.driver = driver;
        mc.url = url;
        mc.login = login;
        mc.password = password;
        mc.limitTyp = limitTyp;
        mc.idNames = idNames;
        mc.sequences = sequences;
        mc.connection = createConnection(driver, url, login, password).connection;
        return mc;
    }

    /**
     * режим datasource
     * когда надо из пула берется соединение
     * в конце соединение отдается в пул
     * для экономии соединений
     * соединение выделяется только тогда, когда она нужно
     * @param dataSource
     * @param limitTyp
     * @param idNames
     * @param sequences
     * @return
     */
    public static MConnection createMConnection(
        DataSource dataSource,
        String limitTyp,
        Map<String, String> idNames,
        Map<String, String> sequences
    ){
        MConnection mc = new MConnection();
        mc.limitTyp = limitTyp;
        mc.idNames = idNames;
        mc.sequences = sequences;
        mc.setDataSource(dataSource);
        return mc;
    }
    //^^createMConnection

    // CRUD
    // CREATE
    public DbData create(DbData dbData, boolean commit) {
        // создает строку в таблицы, значения полей в 'dbData'

        if (dbData == null) {
            return new DbData();
        }
        if (!dbData.isCorrect()) {
            dbData.setDone(false);
            return dbData;
        }

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(commit, "create", "The row will not be create");
        if (!activateResult.done){
            dbData.setDone(false);
            return dbData;
        }

        try {
            Object id = insertRow(dbData);
            dbData.setDone(id != null);
            dbData.setId(id);

            if (commit){
                if (dbData.getDone()){
                    this.commit();
                } else {
                    this.rollback();
                }
            }
        } finally {
            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            } // if
        }
        return dbData;
    }

    // READ
    public DbData read(String tableName, Object id, String fieldNames) {
        return read(tableName, id, splitFieldNames(fieldNames));
    }

    // READ
    public DbData read(String tableName, Object id, String[] fieldNames) {
        // читает поля строки таблицы

        if (fieldNames == null || fieldNames.length == 0) {
            return new DbData(tableName, id);
        }
        if (fieldNames.length == 1 && "*".equals(fieldNames[0])) {
            fieldNames = getMetaDataTable(tableName);
            if (fieldNames == null || fieldNames.length == 0) {
                return new DbData(tableName, id);
            }
        }

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(true, "read", "The row will not be read");
        if (!activateResult.done){
            return new DbData(tableName, id);
        }

        try {
            return readRow(tableName, id, fieldNames);
        } finally {
            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            } // if
            //^^Если соединение бралось из DataSource, то оно возвращается
        } // try
    }

    // UPDATE
    public DbData update(DbData dbData, boolean commit) {
        // модифицирует поля в строке таблицы, значения полей в 'dbData'

        if (dbData == null) {
            return new DbData();
        }
        if (!dbData.isCorrect() || dbData.getId() == null) {
            dbData.setDone(false);
            return dbData;
        }

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(commit, "update", "The row will not be update");
        if (!activateResult.done){
            dbData.setDone(false);
            return dbData;
        }

        try {
            dbData.setDone(updateRow(dbData));

            if (commit){
                if (dbData.getDone()){
                    this.commit();
                } else {
                    this.rollback();
                }
            }
        } finally {
            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            }
        }
        return dbData;
    }

    // DELETE
    public DbData delete(String tableName, Object id, boolean commit) {
        // удаляет строку из таблицы <tableName>, id=<id>

        DbData dbData = new DbData(tableName, id);
        if (id == null) {
            return dbData;
        }

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(commit, "delete", "The row will not be delete");
        if (!activateResult.done){
            dbData.setDone(false);
            return dbData;
        }

        try {
            id = deleteRow(tableName, id);
            dbData.setDone(id != null);
            dbData.setId(id);

            if (commit){
                if (dbData.getDone()){
                    this.commit();
                } else {
                    this.rollback();
                }
            }
        } finally {
            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            } // if
        }
        return dbData;
    }
    //^^CRUD

    // FIND
    public FindData find(String sql, List<Object> params, boolean resultTableNameFlag){
        FindData fd = findPsRows(sql, params, resultTableNameFlag);
        return fd;
    }
    public FindData find(String sql, boolean resultTableNameFlag){
        return find(sql, new ArrayList<>(), resultTableNameFlag);
    }

    public FindData find1(String sql, List<Object> params, boolean resultTableNameFlag) {
        return find(setLimit(sql), params, resultTableNameFlag);
    }
    public FindData find1(String sql, boolean resultTableNameFlag) {
        return find1(sql, new ArrayList<>(), resultTableNameFlag);
    }
    //^^FIND

    public String getIdName(String tableName) {
        if (idNames == null) {
            return null;
        }

        String defaultIdName = getDefaultIdName();
        if (tableName == null || tableName.isEmpty()) {
            return defaultIdName;
        }

        String idn = idNames.get(tableName.toUpperCase());
        if (idn == null) {
            return defaultIdName;
        }
        return idn.toUpperCase();
    }

    public String getDefaultIdName() {
        if (idNames == null) {
            return null;
        }
        String idn = idNames.get(DEFAULT);
        if (idn == null) {
            return null;
        }
        return idn.toUpperCase();
    }

    public String getIdName() {
        return getIdName(null);
    }

    // DataSource
    public DataSource getDataSource() {
        return dataSource;
    }
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public boolean activateDSConnection(boolean autoCommit){
        if ( this.dataSource == null ){
            return false;
        }
        if ( this.connection != null ){
            //    if ( act_pas_warning_flag ){
            //        Utils.outln( "?????????? act_DS_Con: (this.getCon() != null) this.getCon()="+this.getCon() );
            //        try {
            //            int x = 1 / 0;
            //        } catch ( Exception e ){
            //            e.printStackTrace();
            //        } // try
            //    } // if
            return false;
        }

        try {
            this.connection = this.dataSource.getConnection();
            //    if ( act_pas_log_flag ){
            //        Utils.outln( ">>>>>>>>>> act_DS_Con: this.getCon()="+this.getCon() );
            //    } // if
            this.connection.setAutoCommit(autoCommit);
            return true;
        } catch(Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean activateDSConnection(){
        return activateDSConnection( false );
    }

    public void deactivateDSConnection(){
        String conStr = ""+ connection;
        closeDSConnection(connection);
        //    if ( act_pas_log_flag ){
        //        Utils.outln( "<<<<<<<<<< pas_DS_Con(): closeDataSourceCon() this.getCon()="+conStr );
        //    } // if
        this.connection = null;
    }

    public boolean isDataSourceMode(){
        return this.connection == null && this.dataSource != null;
    }

    public ActivateResult actDSCon(boolean commit, String procName, String funcName) {
        ActivateResult activateResult = new ActivateResult();

        if (!isDataSourceMode()) {
            // Режим работы с драйвером 'JDBC' (НЕ с 'DataSource')
            return activateResult;
        }

        // Режим работы с 'DataSource'
        //    if ( act_pas_log_flag ){
        //        Utils.outln( "!!!!!!!!!! isDataSourceMode "+procName+"(..." );
        //    } // if
        if (!commit){
            Utils.outln(
                    "???????????????????????????????????????????????????????????????????????????????"
            );
            Utils.outln( "MConnection."+procName+": It Is Nonsense: (isDataSourceMode() && ! commit)");
            Utils.outln( "   "+funcName );
            Utils.outln(
                    "???????????????????????????????????????????????????????????????????????????????"
            );
        }

        // Взять соединение из DataSource
        activateResult.activated = this.activateDSConnection();
        if (activateResult.activated) {
            return activateResult;
        }

        // не могу взять соединение из DataSource: Выход
        Utils.outln(
                "???????????????????????????????????????????????????????????????????????????????"
        );
        Utils.outln( "MConnection."+procName+": Can't get Connection from DataSource");
        Utils.outln( "   "+funcName );
        Utils.outln(
                "???????????????????????????????????????????????????????????????????????????????"
        );
        activateResult.done = false;

        return activateResult;
    }
    //^^DataSource

    public boolean commit() {
        try {
            this.connection.commit();
            //    if ( this.commitTraceFlag ){
            //        Utils.outln(
            //                "CC MConnection.commit() CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
            //        );
            //    } // if
            return true;
        } catch( Exception ex ) {
            //    Utils.outln( "MConnection.commit: ex=" + ex );
            ex.printStackTrace();
            return false;
        } // try
    } // commit

    public boolean rollback() {
        try {
            this.connection.rollback();
            //    if ( this.rollbackTraceFlag ){
            //        Utils.outln(
            //                "RR MConnection.rollback() RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR"
            //        );
            //    } // if
            return true;
        } catch( Exception ex ) {
            //    Utils.outln( "MConnection.rollback: ex=" + ex );
            ex.printStackTrace();
            return false;
        } // try
    } // rollback

    public String setLimit(String sql, long quant) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        if (LIMIT.equals(this.limitTyp)) {
            if (!sql.contains(" LIMIT ")) {
                return sql + " LIMIT " + quant + " ";
            }
        } else if (FETCH_FIRST.equals(this.limitTyp)) {
            if (!sql.contains(" FETCH FIRST ")){
                return sql + " FETCH FIRST " + quant + " ROWS ONLY ";
            }
        } else if (ROWS.equals(this.limitTyp)) {
            if (!sql.contains(" ROWS ")){
                return sql + " ROWS " + quant + " ";
            }
        } else if (ROWNUM.equals(this.limitTyp)) {
            if (!sql.contains(" ROWNUM <= ")){
                return sql + " ROWNUM <= " + quant + " ";
            }
        } else if (SELECT_FIRST.equals(this.limitTyp)) {
            if (!sql.contains("SELECT FIRST ")) {
                return sql.replace("SELECT FIRST " + quant + " ", "SELECT ");
            }
        }
        return sql;
    }

    public String setLimit(String sql) {
        return setLimit(sql, 1L);
    }

    // con act !!
    protected ResultSet createResultSet( String sqlString ){
        warningNotActivatedDSConnection();
        return createResultSet(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                sqlString
        );
    }

    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------

    // CRUD
    private Object insertRow(DbData dbData) {
        // INSERT INTO PRODUCT(NAME,NUM,ODATE) VALUES(?,?,?);
        StringBuilder columns = new StringBuilder();
        StringBuilder quest = new StringBuilder();
        List<String> fieldNames = dbData.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            columns.append(fieldNames.get(i));
            quest.append("?");
            if ( i < fieldNames.size() - 1) {
                columns.append(",");
                quest.append(",");
            }
        }

        PreparedStatement ps = null;
        Object insertedId = null;
        String tableName = dbData.getTableName();
        StringBuilder sql = new StringBuilder();
        try {
            String idn = this.getIdName(tableName);
            int idIndex = fieldNames.indexOf(idn);
            if (idIndex < 0) {
                // В списке полей нет ID
                insertedId = this.genId(tableName, 1);

                if (insertedId == null) {
                    return null;
                }

                if (fieldNames.isEmpty()) {
                    sql.append("INSERT INTO " + tableName + "( " + idn + " ) VALUES( ? )");
                } else {
                    sql.append("INSERT INTO " + tableName + "( " + idn + "," + columns + " ) VALUES( ?," + quest + " )");
                }
                ps = this.connection.prepareStatement(sql.toString());

                int delta = 2;
                ps.setObject( 1, insertedId);
                List<Object> values = dbData.getValues();
                for ( int i = 0; i < values.size(); i++ ) {
                    Object value = values.get(i);
                    if ( value == null ){
                        ps.setObject( i + delta, value );
                    } else {
                        if ( value.getClass().equals( java.util.Date.class ) ){
                            java.util.Date date = ((java.util.Date) value);
                            ps.setDate(i + delta, new java.sql.Date(date.getTime()));
                        } else {
                            ps.setObject( i + delta, value);
                        }
                    }
                }

                ps.executeUpdate();
                return insertedId;
            } else {
                List<Object> values = dbData.getValues();
                insertedId = values.get(idIndex);

                if (insertedId == null) {
                    return null;
                }

                sql.append("INSERT INTO " + tableName + "( " + columns + " ) VALUES( " + quest + " )");
                ps = this.connection.prepareStatement(sql.toString());

                int delta = 2;
                ps.setObject(1, insertedId);
                for (int i = 0; i < values.size(); i++) {
                    Object value = values.get(i);
                    if ( value == null ){
                        ps.setObject( i + delta, value );
                    } else {
                        if ( value.getClass().equals( java.util.Date.class ) ){
                            java.util.Date date = ((java.util.Date) value);
                            ps.setDate(i + delta, new java.sql.Date(date.getTime()));
                        } else {
                            ps.setObject( i + delta, value);
                        }
                    }
                }
                ps.executeUpdate();
                return insertedId;
            }
        } catch(Exception ex) {
            Utils.outln("MConnection#insertRow: ex = " + ex);
            Utils.outln("  sql=" + sql.toString());
            Utils.outln("  dbData.getValues()=" + dbData.getValues());
            ex.printStackTrace();
            return null;
        } finally {
            closeRsPs(null, ps);
        }
    }

    private DbData readRow(String tableName, Object id, String[] fieldNames) {
        // читает поля строки таблицы

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //-----------------------------------------------------------------
            // SELECT
            //   ID,
            //   NAME,
            //   NUM,
            //   ODATE
            // FROM PRODUCT
            // WHERE
            //   ID = ?
            //-----------------------------------------------------------------
            // SELECT ID,NAME,NUM,ODATE FROM PRODUCT WHERE ID = ?
            //-----------------------------------------------------------------

            // gen sql
            StringBuilder sql = new StringBuilder().append("SELECT ");
            for (int i = 0; i < fieldNames.length; i++) {
                if (i > 0) {
                    sql.append(",");
                }
                sql.append(fieldNames[i]);
            }
            sql.append(" FROM " + tableName + " WHERE " + this.getIdName(tableName) + " = ?");

            // create PreparedStatement
            ps = this.connection.prepareStatement(
                    sql.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY
            );

            // set value
            if (id.getClass().equals( java.util.Date.class)) {
                Date dateVal = (Date) id;
                ps.setDate(1, new java.sql.Date(dateVal.getTime()));
            } else if (id.getClass().equals(Timestamp.class)) {
                ps.setTimestamp(1, (Timestamp) id);
            } else {
                ps.setObject(1, id);
            }

            // query
            rs = ps.executeQuery();

            // read data
            DbData dbData = new DbData(tableName, id);
            if (!rs.next()){
                return dbData;
            }
            for (int i = 0; i < fieldNames.length; i++) {
                Object val = rs.getObject(i + 1);
                dbData.setObject(fieldNames[i], val);
            }
            dbData.setDone(true);

            return dbData;
        } catch (SQLException e){
            e.printStackTrace();
            return new DbData(tableName, id);
        } finally {
            closeRsPs(rs, ps);
        }
    }

    private boolean updateRow(DbData dbData) {
        // UPDATE PRODUCT SET NAME=?,NUM=?,ODATE=? WHERE ID=?

        // gen sql
        List<String> fieldNames = dbData.getFieldNames();
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE " + dbData.getTableName() + " SET ");
        for (int i = 0; i < fieldNames.size(); i++) {
            if (i > 0) {
                sql.append(",");
            }
            sql.append(fieldNames.get(i) + "=?");
        }
        sql.append(" WHERE " + this.getIdName(dbData.getTableName()) + "=?");

        PreparedStatement ps = null;
        //Object insertedId = null;
        try {
            // create PreparedStatement
            ps = this.connection.prepareStatement(sql.toString());

            // set data
            List<Object> values = dbData.getValues();
            int delta = 1;
            for (int i = 0; i < values.size(); i++) {
                Object value = values.get(i);
                if ( value == null ){
                    ps.setObject( i + delta, value );
                } else if ( value.getClass().equals( java.util.Date.class ) ){
                    java.util.Date date = ((java.util.Date) value);
                    ps.setDate(i + delta, new java.sql.Date(date.getTime()));
                } else {
                    ps.setObject( i + delta, value);
                }
            }
            ps.setObject(values.size() + delta, dbData.getId());

            // query
            ps.executeUpdate();
            return true;
        } catch(Exception ex) {
            Utils.outln("MConnection#updateRowRow: ex = " + ex);
            Utils.outln("  sql=" + sql.toString());
            Utils.outln("  dbData.getValues()=" + dbData.getValues());
            ex.printStackTrace();
            return false;
        } finally {
            closeRsPs(null, ps);
        }
    }

    private Object deleteRow(String tableName, Object id) {
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(
                    "DELETE FROM " + tableName + " WHERE " + this.getIdName(tableName) + " = ?"
            );
            ps.setObject(1, id);
            ps.executeUpdate();
            return id;
        } catch( Exception ex ) {
            Utils.outln("deleteRow: ex = " + ex);
            ex.printStackTrace();
            return null;
        } finally {
            closeRsPs(null, ps);
        }
    }
    //^^CRUD

    // FIND
    private FindData findPsRows(String sql, List<Object> params, boolean resultTableNameFlag){
        FindData fd = new FindData();

        if (sql == null || sql.isEmpty()){
            fd.setExStr(
                "------------------------------------------------------------------------------\r\n"+
                "?? findPsRows: sql == null || sql.isEmpty()\r\n"+
                sql+"\r\n"+
                "params="+params+"\r\n"+
                "------------------------------------------------------------------------------"
            );
            return fd;
        } // if

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(true, "read", "The row will not be read");
        if (!activateResult.done){
            fd.setExStr(
                "------------------------------------------------------------------------------\r\n"+
                "?? findPsRows: actDSCon\r\n"+
                sql+"\r\n"+
                "params="+params+"\r\n"+
                "------------------------------------------------------------------------------"
            );
            return fd;
        }

        PreparedStatement[] ps = {null};
        ResultSet rs = null;
        try {
            rs = this.createResultSetPs(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY,
                    sql,
                    params,
                    ps
            );
            if ( rs == null ){
                fd.setExStr(
                    "------------------------------------------------------------------------------\r\n"+
                    "?? findPsRows: rs == null\r\n"+
                    sql+"\r\n"+
                    "params="+params+"\r\n"+
                    "------------------------------------------------------------------------------"
                );
                return fd;
            } // if

            // gen result
            List<DbData> dbdList = new ArrayList<>();
            String[] fns = getFieldNamesFromRs(rs, resultTableNameFlag);
            rs.beforeFirst();
            while (rs.next()) {
                DbData dbd = new DbData();
                for ( int i = 0; i < fns.length; i++){
                    // TODO в normRsObject вставить компенсацию возможного искажения времени из-за 'TimeZone'
                    //values[i] = MConnection.normRsObject( rs.getObject( i + 1 ) );
                    Object values = rs.getObject( i + 1);

                    dbd.setObject(fns[i].toUpperCase(), values);
                }
                dbd.setDone(true);
                dbdList.add(dbd);
            }
            fd.setDbDatas(dbdList);

            return fd;
        } catch (Exception ex) {
            ex.printStackTrace();
            fd.setExStr(
                "------------------------------------------------------------------------------\r\n"+
                "?? findPsRows: Exception ex=" + ex +"\r\n"+
                sql+"\r\n"+
                "params="+params+"\r\n"+
                "------------------------------------------------------------------------------"
            );
            return fd;
        } finally {
            closeRsPs(rs, ps[0]);

            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            } // if
        } // try
    }

    // con act !!
    public ResultSet createResultSetPs(
            int resultSetType,
            int resultSetConcurency,
            String sql,
            List<Object> values,
            PreparedStatement[] ps
    ){
        warningNotActivatedDSConnection();

        if (sql == null) {
            return null;
        }

        if (ps == null || ps.length == 0){
            ps = new PreparedStatement[1];
        }
        ps[0] = null;

        try {
            if (resultSetType == -1 || resultSetConcurency == -1) {
                ps[0] = this.connection.prepareStatement(
                        sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            } else {
                ps[0] = this.connection.prepareStatement(sql, resultSetType, resultSetConcurency );
            }

            if (values != null) {
                for (int i = 0; i < values.size(); i++) {
                    int ind = i + 1;
                    Object val = values.get(i);
                    if (val == null) {
                        ps[0].setObject(ind, val);
                    } else {
                        if (val.getClass().equals( java.util.Date.class)) {
                            Date dateVal = (Date) val;
                            ps[0].setDate( ind, new java.sql.Date(dateVal.getTime()));
                        } else if (val.getClass().equals(Timestamp.class)) {
                            ps[0].setTimestamp(ind, (Timestamp) val);
                        } else {
                            ps[0].setObject(ind, val);
                        }
                    }
                }
            }
            return ps[0].executeQuery();
        } catch(SQLException ex) {
            Utils.outln("MConnection#createResultSetPs: ex = " + ex);
            Utils.outln("sql=" + sql);
            ex.printStackTrace();

            // ps[0] закрывается, только если Exception
            closeRsPs(null, ps[0]);
            return null;
        }
    }

    private String[] getFieldNamesFromRs(ResultSet rs, boolean tableNameFlag) {
        try {
            String[] fc = new String[rs.getMetaData().getColumnCount()];
            for ( int i = 0; i < fc.length; i++ ) {
                int ic = i + 1;

                String tn = null;
                if (tableNameFlag) {
                    tn = rs.getMetaData().getTableName(ic);
                }

                String fn = rs.getMetaData().getColumnName(ic);
                if (tn != null && !tn.isEmpty()) {
                    fn = tn + "." + fn;
                }

                fc[i] = fn;
            }
            return fc;
        } catch (SQLException e) {
            return null;
        }
    }
    //^^FIND

    // MetaData
    // con pas
    private String[] getMetaDataTable(String tableName){
        String[] mt = (String[])(metaDataTable_Cash.get(tableName));
        if ( mt == null ){
            //mt = queryMetaDataTable(tableName);
            mt = getMetaDataFields(setLimit("SELECT * FROM " + tableName));
            if ( mt != null ){
                //metaDataTable_Cash.put(mt, tableName);
                metaDataTable_Cash.put(tableName, mt);
            } // if
        } // if

        return mt;
    }

    // con ++
    private String[] getMetaDataFields(String sql){
        if (sql == null){
            return null;
        }

        ResultSet rs = null;
        try {
            rs = createResultSet(sql);
            if ( rs == null ) {
                return null;
            }

            int qCol = rs.getMetaData().getColumnCount();

            String[] fc = new String[qCol];
            for (int i = 0; i < qCol; i++) {
                fc[i] = rs.getMetaData().getColumnName(i + 1);
                if ( fc[i] != null){
                    fc[i] = fc[i].toUpperCase();
                } // if
            }

            return fc;
        } catch (SQLException ex){
            ex.printStackTrace();
            return null;
        } finally {
            this.closeResultSetAndStatement(rs);
        }
    }
    //^^MetaData

    // ResultSet
    private ResultSet createResultSet(Statement[] stmt, String sqlString){
        CRSF = null;
        if ( stmt == null ){
            CRSF = "stmt == null";
            Utils.outln(
                    "??????????????????????????????????????????????????????????????????\r\n"+
                            "MConnection.createResultSet: Ошибка SQL запроса:\r\n" +
                            "stmt == null" + "\r\n\r\n" +
                            sqlString
            );
            return null;
        } // if
        if ( stmt.length == 0 ){
            CRSF = "stmt.length";
            Utils.outln(
                    "??????????????????????????????????????????????????????????????????\r\n"+
                            "MConnection.createResultSet: Ошибка SQL запроса:\r\n" +
                            "stmt.length == 0" + "\r\n\r\n" +
                            sqlString
            );
            return null;
        } // if
        if ( stmt[ 0 ] == null ){
            CRSF = "stmt[ 0 ] == null";
            Utils.outln(
                    "??????????????????????????????????????????????????????????????????\r\n"+
                            "MConnection.createResultSet: Ошибка SQL запроса:\r\n" +
                            "stmt[ 0 ] == null" + "\r\n\r\n" +
                            sqlString
            );
            return null;
        } // if

        ResultSet rs = null;

        // TODO
        //String sqlExt = this.extSql( sqlString );
        String sqlExt = sqlString;
        try {
            rs = stmt[ 0 ].executeQuery( sqlExt );
            return rs;
        } catch( java.sql.SQLSyntaxErrorException see ){
            CRSF = IS_SEE__PREFIX + see;
            Utils.outln( "Ошибка SQL запроса:" );
            Utils.outln( IS_SEE__PREFIX + see );
            Utils.outln( "--sqlString-----------------------------------------------" );
            Utils.outln( "sqlString=" + sqlString );
            Utils.outln( "sqlExt=" + sqlString );
            Utils.outln( "----------------------------------------------------------" );
            see.printStackTrace();
            return null;
        } catch(SQLException e){
            CRSF = IS_REL_NOT_EXIST__PREFIX + e;
            Utils.outln( "Ошибка SQL запроса:" );
            Utils.outln( IS_REL_NOT_EXIST__PREFIX + e );
            Utils.outln( "--sqlString-----------------------------------------------" );
            Utils.outln( "sqlString=" + sqlString );
            Utils.outln( "sqlExt=" + sqlString );
            Utils.outln( "----------------------------------------------------------" );
            //e.printStackTrace();
            return null;
        }
    }

    // con act !!
    private ResultSet createResultSet(int resultSetType, int resultSetConcurency, String sqlString){
        warningNotActivatedDSConnection();

        Statement[] stmt = {
            this.createStatement(resultSetType, resultSetConcurency)
        };
        if (stmt[0] == null){
            return null;
        }
        return this.createResultSet(stmt, sqlString);
    }

    // con act !!
    private Statement createStatement(int resultSetType, int resultSetConcurency){
        //---------------------------
        //  resultSetType:
        //    TYPE_SCROLL_SENSITIVE,
        //    TYPE_FORWARD_ONLY,
        //    TYPE_SCROLL_INSENSITIVE
        //---------------------------
        //  resultSetConcurency:
        //    CONCUR_READ_ONLY
        //    CONCUR_UPDATABLE
        //---------------------------
        warningNotActivatedDSConnection();

        Statement stmt = null;
        try {
            stmt = this.connection.createStatement( resultSetType, resultSetConcurency );
            return stmt;
        } catch(SQLException ex){
            Utils.outln("MConnection#createStatement: ex=" + ex);
            ex.printStackTrace();
            return null;
        }
    }

    private static void closeRsPs(ResultSet rs, PreparedStatement ps){
        try {
            if ( rs != null ){
                rs.close();
            }
            if ( ps != null ){
                ps.close();
            }
        } catch ( Exception e ){
            e.printStackTrace();
        }
    }

    private static void closeResultSetAndStatement(ResultSet rs) {
        if (rs == null || isRsNotValid(rs)) {
            return;
        }

        try {
            Statement stmt = rs.getStatement();
            rs.close();
            if (stmt != null){
                stmt.close();
            }
        } catch (SQLException ex) {
            Utils.outln("MConnection#closeResultSetAndStatement: ex=" + ex);
            ex.printStackTrace();
        }
    }

    private static boolean isRsNotValid(ResultSet rs){
        if (rs == null) {
            return true;
        }
        try {
            rs.beforeFirst();
            return false;
        } catch (SQLException ex){
            return true;
        }
    }
    //^^ResultSet

    // DataSource
    private void closeDSConnection(Connection connection){
        if ( connection == null ){
            //    if ( act_pas_warning_flag ){
            //        Utils.outln( "?????????? MConnection.closeDataSourceCon: con == null" );
            //        try {
            //            int x = 1 / 0;
            //        } catch ( Exception e ){
            //            e.printStackTrace();
            //        } // try
            //    } // if
            return;
        } // if

        try {
            //String str = connection.toString();
            connection.close();
        } catch( Exception e ){
            e.printStackTrace();
        } // try
    }

    private boolean warningNotActivatedDSConnection(){
        if (this.connection == null){
            Utils.outln(
                    "???????????????????????????????????????????????????????????????????????????????"
            );
            Utils.outln( "MConnection.warning_DS_con_act: ( this.getCon() == null )" );
            Utils.outln(
                    "???????????????????????????????????????????????????????????????????????????????"
            );

            try {
                int x = 1 / 0;
            } catch ( Exception e ){
                e.printStackTrace();
            }

            return false;
        } else {
            return true;
        } // if
    }
    //^^DataSource

    private static String[] splitFieldNames(String fieldNames) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            return null;
        }

        fieldNames = fieldNames.trim();
        if (fieldNames.isEmpty()) {
            return null;
        }

        String[] fns = fieldNames.split(DELIM);
        for (int i = 0; i < fns.length; i++) {
            fns[i] = fns[i].trim();
        }
        return fns;
    }

    // gen Id
    private Object genId(String tableName, int delta){
        if (this.sequences == null){
            // TABLES
            return genIdLoc(tableName, delta);
        } else {
            // sequences (Postgres)
            return pqsqlSeqNextval(sequences.get(tableName));
        }
    }

    private Object pqsqlSeqNextval(String seqName){
        if (seqName == null || seqName.isEmpty()){
            return null;
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = this.connection.prepareStatement( "SELECT nextval( '" + seqName + "' )" );
            rs = ps.executeQuery();
            if (rs.next()){
                return rs.getObject( 1 );
            } else {
                return null;
            }
        } catch ( Exception ex ){
            ex.printStackTrace();
            return null;
        } finally {
            closeRsPs(rs, ps);
        }
    }

    private Long genIdLoc(String tableName, int delta){
        // ....................................................................
        // UPDATE TABLES
        // SET
        //   GENERATOR = GENERATOR + 1
        // WHERE
        //   NAME = ?
        // ....................................................................
        // UPDATE " + TABLES + " SET GENERATOR = GENERATOR + " + Delta + " WHERE Name = ?
        // ....................................................................

        if ( tableName == null || tableName.isEmpty() || delta == 0){
            return null;
        }

        Long gen = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // update
            ps = this.connection.prepareStatement(
                    "UPDATE " + TABLES + " SET GENERATOR = GENERATOR + "+ delta + " WHERE Name = ?"
            );
            ps.setString( 1, tableName.toUpperCase() );
            ps.executeUpdate();
            ps.close();
            //^^update

            // get
            ps = this.connection.prepareStatement(
                    "SELECT Generator FROM "+TABLES+" WHERE ( Name = ? )"
            );
            ps.setObject( 1, TABLES.toUpperCase() );
            rs = ps.executeQuery();
            rs.next();
            gen = rs.getLong( "GENERATOR" );
            //^^get

            return gen;
        } catch ( Exception ex ) {
            ex.printStackTrace();
            return null;
        } finally {
            closeRsPs(rs, ps);
        }
    }
    //^^gen Id

    private static class ConnectionReply {
        protected Connection connection = null;
        protected String error = null;
    }

    private static ConnectionReply createConnection(String driver, String url, String login, String password){
        ConnectionReply connectionReply = new ConnectionReply();
        try {
            Class.forName( driver );

            Locale locale = Locale.getDefault();
            Locale.setDefault( Locale.US );
            connectionReply.connection = DriverManager.getConnection(url, login, password);
            Locale.setDefault( locale );

            connectionReply.connection.setAutoCommit( false );
        } catch( Exception e ){
            e.printStackTrace();
            connectionReply.connection = null;
            connectionReply.error = e.toString();
        } // try
        return connectionReply;
    }

    //    private boolean[] initBoolean(boolean[] b) {
    //        if (b == null || b.length < 1) {
    //            b = new boolean[1];
    //        }
    //        b[0] = false;
    //        return b;
    //    }

    //    private static String[] initString(String[] val) {
    //        if (val == null || val.length < 1) {
    //            val = new String[1];
    //        }
    //        val[0] = null;
    //        return val;
    //    }

    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------

    private static MConnection testCreateMc() {
        Map<String, String> idNames = new HashMap<>();
        idNames.put(DEFAULT, ID);
/*        idNames.put("AUDC_PCLIB_BUSINESS_OBJECT", "SYSNAME");
        idNames.put("AUDC_PCLIB_BUSINESS_OPERATIONS", "SYSNAME");
        idNames.put("AUDC_PCLIB_BUSINESS_SERVICE", "SYSNAME");
        idNames.put("AUDC_TASK_PARAM", "SYSNAME");*/
//        oboz2.public.flyway_schema_history [PostgreSQL - oboz2@localhost]
        Map<String, String> sequences = new HashMap<>();
/*        sequences.put("AUDC_ACLIB_ACTOR_ACCOUNT", "audc_aclib_actor_account_id_seq");
        sequences.put("AUDC_ACLIB_CLIENT_IP_RANGE", "audc_aclib_client_ip_range_id_seq");
        sequences.put("AUDC_ACLIB_ROLE_WORKPLACE", "audc_aclib_role_workplace_id_seq");
        sequences.put("AUDC_PARAM", "audc_param_seq");
        sequences.put("AUDC_TASK", "audc_task_seq");
        sequences.put("AUDC_USER", "audc_user_seq");*/

        MConnection mc = MConnection.createMConnection(
            "org.postgresql.Driver",
            "jdbc:postgresql://127.0.0.1:5432/oboz2",
            "oboz2",
            "uqu4Ahtu",
            LIMIT,
            idNames,
            sequences
        );
        return mc;
    }

    private static DbData testCreateRow(MConnection mc, String name, String val) {
        DbData dbd = new DbData("AUDC_PARAM");
        dbd.setString("CURATOR_ACTION", "ARCHIVE");
        dbd.setString("DEFAULT_VALUE", val);
        dbd.setString("PARAM_DESCR", "Период создания нового индекса");
        dbd.setString("PARAM_NAME", name);
        dbd.setString("PARAM_TYPE", "INT");
        dbd = mc.create(dbd, true);
        if (dbd.getDone()) {
            Utils.outln("-- Создана строка: dbd=" + dbd.toStr());
        } else {
            Utils.outln("?? Не могу создать строку: dbd=" + dbd.toStr());
        }
        return dbd;
    }

    private static List<DbData> testCreateRows(MConnection mc) {
        List<DbData> list = new ArrayList<>(3);
        list.add(testCreateRow(mc, "rollover_period_1", "1"));
        list.add(testCreateRow(mc, "rollover_period_2", "2"));
        list.add(testCreateRow(mc, "rollover_period_3", "3"));
        return list;
    }

    private static List<DbData> testReadRows(MConnection mc, List<DbData> list) {
        List<DbData> listResult = new ArrayList<>(3);
        for (DbData dbData : list) {
            if (dbData.getDone()) {
                DbData dbdResult = mc.read(dbData.getTableName(), dbData.getId(), "*");
                if (dbdResult.getDone()) {
                    Utils.outln("-- Прочитана строка: dbdResult=" + dbdResult.toStr());
                } else {
                    Utils.outln("?? Не могу прочитать строку: dbdResult=" + dbdResult.toStr());
                }
                listResult.add(dbdResult);
            }
        }
        return listResult;
    }

    private static DbData testUpdateRow(MConnection mc, Object id, String name, String val, String type) {
        DbData dbd = new DbData("AUDC_PARAM", id);
        dbd.setString("DEFAULT_VALUE", val);
        dbd.setString("PARAM_NAME", name);
        dbd.setString("PARAM_TYPE", type);
        dbd = mc.update(dbd, true);
        if (dbd.getDone()) {
            Utils.outln("-- Модифицирована строка: dbd=" + dbd.toStr());
        } else {
            Utils.outln("?? Не могу модифицировать строку: dbd=" + dbd.toStr());
        }
        return dbd;
    }

    private static List<DbData> testUpdateRows(MConnection mc, List<DbData> list) {
        List<DbData> listRes = new ArrayList<>(3);
        listRes.add(testUpdateRow(mc, list.get(0).getId(), "rollover_period_1_@", "11", "TYPE1"));
        listRes.add(testUpdateRow(mc, list.get(1).getId(), "rollover_period_2_@", "21", "TYPE2"));
        listRes.add(testUpdateRow(mc, list.get(2).getId(), "rollover_period_3_@", "31", "TYPE3"));
        return list;
    }

    private static List<DbData> testFindRows(MConnection mc, String sql, boolean tnFlag) {
        FindData fd = mc.find(sql, tnFlag);

        if (fd.getQuant() <= 0) {
            Utils.outln("?? Строки не найдены: sql=" + sql);
            return fd.getDbDatas();
        }
        for (DbData dbData : fd.getDbDatas()) {
            Utils.outln("-- Найденная строка: dbData=" + dbData.toStr());
        }
        return fd.getDbDatas();
    }

    private static List<DbData> testFindRows(MConnection mc, String sql, List<Object> params, boolean tnFlag) {
        FindData fd = mc.find(sql, params, tnFlag);

        if (fd.getQuant() <= 0) {
            Utils.outln("?? Строки не найдены: sql=" + sql);
            return fd.getDbDatas();
        }
        for (DbData dbData : fd.getDbDatas()) {
            Utils.outln("-- Найденная строка: dbData=" + dbData.toStr());
        }
        return fd.getDbDatas();
    }

    private static List<DbData> testDeleteRows(MConnection mc, List<DbData> list) {
        List<DbData> listResult = new ArrayList<>(3);
        for (DbData dbData : list) {
            if (dbData.getDone()) {
                DbData dbdResult = mc.delete(dbData.getTableName(), dbData.getId(), true);
                if (dbdResult.getDone()) {
                    Utils.outln("-- Удалена строка: dbdResult=" + dbdResult.toStr());
                } else {
                    Utils.outln("?? Не могу удалить строку: dbdResult=" + dbdResult.toStr());
                }
                listResult.add(dbdResult);
            }
        }
        return listResult;
    }

    private static void test() {
        MConnection mc = testCreateMc();
        Utils.outln("mc=" + mc);
        Utils.outln("mc.connection=" + mc.connection);
        Utils.outln("--------------------------------------------------------------------------------");

        String sql =
                "SELECT " +
                        " *  " +
                        "FROM dictionaries.currencies " ;
        List<DbData> findList5 = testFindRows(mc, sql, true);
        Utils.outln("findList5=" + findList5);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> createdList = testCreateRows(mc);
        Utils.outln("createdList=" + createdList);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> readList = testReadRows(mc, createdList);
        Utils.outln("readList=" + readList);
        Utils.outln("--------------------------------------------------------------------------------");
   /*
        List<DbData> updateList = testUpdateRows(mc, createdList);
        Utils.outln("updateList=" + updateList);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> readList2 = testReadRows(mc, updateList);
        Utils.outln("readList2=" + readList2);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> findList = testFindRows(mc, "SELECT * FROM AUDC_PARAM ORDER BY ID", false);
        Utils.outln("findList=" + findList);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> findList2 = testFindRows(mc,
                "SELECT * FROM AUDC_PARAM " +
                     "WHERE "+
                       "PARAM_NAME LIKE '%_@' "+
                     "ORDER BY ID",
                true
        );
        Utils.outln("findList2=" + findList2);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> findList3 = testFindRows(mc,
                "SELECT " +
                        "CURATOR_ACTION, "+
                        "DEFAULT_VALUE, "+
                        "PARAM_DESCR "+
                     "FROM AUDC_PARAM " +
                        "WHERE "+
                     "PARAM_NAME LIKE '%_@' "+
                     "ORDER BY ID",
                false
        );
        Utils.outln("findList3=" + findList3);
        Utils.outln("--------------------------------------------------------------------------------");

        List<Object> params = new ArrayList<>();
        params.add("%_@");
        List<DbData> findList4 = testFindRows(mc,
        "SELECT " +
                "CURATOR_ACTION, "+
                "DEFAULT_VALUE, "+
                "PARAM_DESCR "+
              "FROM AUDC_PARAM " +
              "WHERE "+
                "PARAM_NAME NOT LIKE ? "+
              "ORDER BY ID",
              params,
             false
        );
        Utils.outln("findList4=" + findList4);
        Utils.outln("--------------------------------------------------------------------------------");

        String sql =
            "SELECT " +
                "AUDC_TASK_PARAM.PARAM_VALUE, AUDC_TASK.*, AUDC_PARAM.* " +
            "FROM AUDC_TASK_PARAM " +
            "LEFT JOIN AUDC_TASK  ON AUDC_TASK.ID  = AUDC_TASK_PARAM.TASK_ID " +
            "LEFT JOIN AUDC_PARAM ON AUDC_PARAM.ID = AUDC_TASK_PARAM.PARAM_ID";
        List<DbData> findList5 = testFindRows(mc, sql, true);
        Utils.outln("findList5=" + findList5);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> deletedList = testDeleteRows(mc, createdList);
        Utils.outln("deletedList=" + deletedList);
        Utils.outln("--------------------------------------------------------------------------------");
  */  }

    public static void main(String[] args) {
        test();
    }
}
