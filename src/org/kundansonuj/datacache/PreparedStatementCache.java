package org.kundansonuj.datacache;


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.SortedMap;
import java.util.TreeMap;

import org.kundansonuj.datacache.redis.RedisClient;
import org.kundansonuj.datacache.redis.SqlUtil;




public class PreparedStatementCache implements PreparedStatement {
    private Connection parentConnection;
    private PreparedStatement wrappedPreparedStatement;
    private RedisClient redisClient;
    private String database;
    private String statementSql;
    private SortedMap<Integer, Object> variables = new TreeMap<>();
    private boolean cacheable = true;

    public PreparedStatementCache(Connection parentConnection, PreparedStatement wrappedPreparedStatement, String statementSql, RedisClient redisClient,String database) {
        this.parentConnection = parentConnection;
        this.wrappedPreparedStatement = wrappedPreparedStatement;
        this.statementSql = SqlUtil.tokenizeStatement(statementSql);
        this.redisClient = redisClient;
        this.database=database;
    }

    public void closeOnCompletion() throws SQLException {
        wrappedPreparedStatement.closeOnCompletion();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return wrappedPreparedStatement.isCloseOnCompletion();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (    "java.sql.PreparedStatement".equals(iface.getName())
                    || "java.sql.Statement".equals(iface.getName())
                    || "java.sql.Wrapper.class".equals(iface.getName())) {
                return iface.cast(this);
            }

            return wrappedPreparedStatement.unwrap(iface);
        } catch (ClassCastException cce) {
            throw new SQLException("Unable to unwrap to " + iface.toString(), cce);
        }
    }

    public boolean isWrapperFor(Class iface) throws SQLException {
        if (    "java.sql.PreparedStatement".equals(iface.getName())
                || "java.sql.Statement".equals(iface.getName())
                || "java.sql.Wrapper.class".equals(iface.getName())) {
            return true;
        }
        return wrappedPreparedStatement.isWrapperFor(iface);
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return redisClient.executeQuery(this.wrappedPreparedStatement, sql,database);
    }

    public ResultSet executeQuery() throws SQLException {
        // Compute the SQL for the prepared statement using the tokenized SQL string and the
        // variables values
    	System.out.println("Is Cacheable::"+cacheable);
        String query = this.cacheable ?
                MessageFormat.format(this.statementSql, this.variables.values().toArray()) : null;
     //  System.out.println("sqlquery"+query);
    /*   try {
		query=MD5Generator.getMD5(query);
	} catch (HashGenerationException e) {
		e.printStackTrace();
	}*/
                return redisClient.executeQuery(this.wrappedPreparedStatement,query,database );
    }

    public boolean execute() throws SQLException {
        return wrappedPreparedStatement.execute();
    }

    public boolean execute(String sql) throws SQLException {
        return wrappedPreparedStatement.execute(sql);
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return wrappedPreparedStatement.execute(sql, autoGeneratedKeys);
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return wrappedPreparedStatement.execute(sql, columnIndexes);
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return wrappedPreparedStatement.execute(sql, columnNames);
    }

    public ResultSet getResultSet() throws SQLException {
        return wrappedPreparedStatement.getResultSet();
    }

    public int executeUpdate(String sql) throws SQLException {
        return wrappedPreparedStatement.executeUpdate(sql);
    }

    public void close() throws SQLException {
        wrappedPreparedStatement.close();
    }

    public int getMaxFieldSize() throws SQLException {
        return wrappedPreparedStatement.getMaxFieldSize();
    }

    public void setMaxFieldSize(int max) throws SQLException {
        wrappedPreparedStatement.setMaxFieldSize(max);
    }

    public int getMaxRows() throws SQLException {
        return wrappedPreparedStatement.getMaxRows();
    }

    public void setMaxRows(int max) throws SQLException {
        wrappedPreparedStatement.setMaxRows(max);
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        wrappedPreparedStatement.setEscapeProcessing(enable);
    }

    public int getQueryTimeout() throws SQLException {
        return wrappedPreparedStatement.getQueryTimeout();
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        wrappedPreparedStatement.setQueryTimeout(seconds);
    }

    public void cancel() throws SQLException {
        wrappedPreparedStatement.cancel();
    }

    public SQLWarning getWarnings() throws SQLException {
        return wrappedPreparedStatement.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        wrappedPreparedStatement.clearWarnings();
    }

    public void setCursorName(String name) throws SQLException {
        wrappedPreparedStatement.setCursorName(name);
    }

    public int getUpdateCount() throws SQLException {
        return wrappedPreparedStatement.getUpdateCount();
    }

    public boolean getMoreResults() throws SQLException {
        return wrappedPreparedStatement.getMoreResults();
    }

    public void setFetchDirection(int direction) throws SQLException {
        wrappedPreparedStatement.setFetchDirection(direction);
    }

    public int getFetchDirection() throws SQLException {
        return wrappedPreparedStatement.getFetchDirection();
    }

    public void setFetchSize(int rows) throws SQLException {
        wrappedPreparedStatement.setFetchSize(rows);
    }

    public int getFetchSize() throws SQLException {
        return wrappedPreparedStatement.getFetchSize();
    }

    public int getResultSetConcurrency() throws SQLException {
        return wrappedPreparedStatement.getResultSetConcurrency();
    }

    public int getResultSetType() throws SQLException {
        return wrappedPreparedStatement.getResultSetType();
    }

    public void addBatch(String sql) throws SQLException {
        wrappedPreparedStatement.addBatch(sql);
    }

    public void clearBatch() throws SQLException {
        wrappedPreparedStatement.clearBatch();
    }

    public int[] executeBatch() throws SQLException {
        return wrappedPreparedStatement.executeBatch();
    }

    public Connection getConnection() throws SQLException {
        return parentConnection;
    }

    public boolean getMoreResults(int current) throws SQLException {
        return wrappedPreparedStatement.getMoreResults(current);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return wrappedPreparedStatement.getGeneratedKeys();
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return wrappedPreparedStatement.executeUpdate(sql, autoGeneratedKeys);
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return wrappedPreparedStatement.executeUpdate(sql, columnIndexes);
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return wrappedPreparedStatement.executeUpdate(sql, columnNames);
    }

    public int getResultSetHoldability() throws SQLException {
        return wrappedPreparedStatement.getResultSetHoldability();
    }

    public boolean isClosed() throws SQLException {
        return wrappedPreparedStatement.isClosed();
    }

    public void setPoolable(boolean poolable) throws SQLException {
        wrappedPreparedStatement.setPoolable(poolable);
    }

    public boolean isPoolable() throws SQLException {
        return wrappedPreparedStatement.isPoolable();
    }

    public int executeUpdate() throws SQLException {
        return wrappedPreparedStatement.executeUpdate();
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.variables.put(parameterIndex - 1, "null");
        wrappedPreparedStatement.setNull(parameterIndex, sqlType);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setBoolean(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setByte(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setShort(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setInt(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setLong(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setFloat(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setDouble(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setBigDecimal(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        this.variables.put(parameterIndex - 1, x);
        wrappedPreparedStatement.setString(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.variables.put(parameterIndex - 1, new String(x));
        wrappedPreparedStatement.setBytes(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setDate(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setTime(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setTimestamp(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setAsciiStream(parameterIndex, x, length);
    }

    @SuppressWarnings("deprecation")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setUnicodeStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setBinaryStream(parameterIndex, x, length);
    }

    public void clearParameters() throws SQLException {
        wrappedPreparedStatement.clearParameters();
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setObject(parameterIndex, x, targetSqlType);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setObject(parameterIndex, x);
    }

    public void addBatch() throws SQLException {
        wrappedPreparedStatement.addBatch();
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setRef(parameterIndex, x);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setBlob(parameterIndex, x);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setClob(parameterIndex, x);
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setArray(parameterIndex, x);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return wrappedPreparedStatement.getMetaData();
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException { //TODO
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setDate(parameterIndex, x, cal);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException { //TODO
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setTime(parameterIndex, x, cal);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException { //TODO
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setTimestamp(parameterIndex, x, cal);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.variables.put(parameterIndex - 1, "null");
        wrappedPreparedStatement.setNull(parameterIndex, sqlType, typeName);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.variables.put(parameterIndex - 1, String.valueOf(x));
        wrappedPreparedStatement.setURL(parameterIndex, x);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return wrappedPreparedStatement.getParameterMetaData();
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setRowId(parameterIndex, x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        this.variables.put(parameterIndex - 1, value);
        wrappedPreparedStatement.setNString(parameterIndex, value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setNCharacterStream(parameterIndex, value, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setNClob(parameterIndex, value);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setClob(parameterIndex, reader, length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setBlob(parameterIndex, inputStream, length);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setNClob(parameterIndex, reader, length);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setSQLXML(parameterIndex, xmlObject);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setAsciiStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setBinaryStream(parameterIndex, x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setAsciiStream(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setBinaryStream(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setCharacterStream(parameterIndex, reader);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setNCharacterStream(parameterIndex, value);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setClob(parameterIndex, reader);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setBlob(parameterIndex, inputStream);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.cacheable = false;
        wrappedPreparedStatement.setNClob(parameterIndex, reader);
    }
}