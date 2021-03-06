package net.unit8.solr.jdbc.impl;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.unit8.solr.jdbc.command.Command;
import net.unit8.solr.jdbc.command.CommandFactory;
import net.unit8.solr.jdbc.message.DbException;
import net.unit8.solr.jdbc.message.ErrorCode;

import java.io.StringReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class StatementImpl implements Statement {
	/**
	 * This parameter is used to paginate results from a query.
	 */
	private static final int MAX_FETCH_SIZE = 10000;

	protected SolrConnection conn;
	protected AbstractResultSet resultSet;
	protected int updateCount;

	private List<String> batchCommands;
	private boolean isClosed = false;
	private int maxRows;
	protected boolean escapeProcessing = true;

	protected int fetchSize = MAX_FETCH_SIZE;
	protected final int resultSetType;
	protected final int resultSetConcurrency;
	protected boolean closedByResultSet;

	public StatementImpl(SolrConnection conn, int resultSetType, int resultSetConcurrency) {
		this.conn = conn;
		this.resultSetType = resultSetType;
		this.resultSetConcurrency = resultSetConcurrency;
	}

	private Command parseSQL(String sql) throws SQLException {
		try {
            Command command = null;
            CCJSqlParserManager pm = new CCJSqlParserManager();
			net.sf.jsqlparser.statement.Statement statement = pm.parse(new StringReader(sql));
			command = CommandFactory.getCommand(statement);
			command.setConnection(conn);
			command.parse();
            return command;
        } catch (JSQLParserException ex) {
			throw DbException.get(ErrorCode.SYNTAX_ERROR, ex, ex.getMessage()).getSQLException();
		}
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		checkClosed();
		if (batchCommands == null) {
			batchCommands = new ArrayList<String>();
		}
		batchCommands.add(sql);

	}

	@Override
	public void cancel() throws SQLException {
	}

	@Override
	public void clearBatch() throws SQLException {
        checkClosed();
        batchCommands = null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		checkClosed();
		// Do nothing
	}

	@Override
	public void close() throws SQLException {
		conn = null;
		isClosed = true;
	}

	private boolean executeInternal(String sql) throws SQLException {
		Command command = parseSQL(sql);
		try {
			if(command.isQuery()) {
				resultSet = command.executeQuery();
			} else {
				updateCount = command.executeUpdate();
				this.conn.setUpdatedInTx(true);
			}
		} catch (DbException e) {
			throw e.getSQLException();
		}
		return true;
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		checkClosed();
		return executeInternal(sql);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		checkClosed();
		return executeInternal(sql);
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		checkClosed();
		return executeInternal(sql);
	}

	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		checkClosed();
		return executeInternal(sql);
	}

	@Override
	public int[] executeBatch() throws SQLException {
		checkClosed();
		if (batchCommands == null) {
			batchCommands = new ArrayList<String>();
		}
		int[] result = new int[batchCommands.size()];
		boolean error = false;

		for (int i = 0; i < batchCommands.size(); i++) {
			String sql = batchCommands.get(i);
			try {
				result[i] = executeUpdate(sql);
			} catch(Exception e) {
				logAndConvert(e);
				result[i] = Statement.EXECUTE_FAILED;
				error = true;
			}
		}
		batchCommands = null;
		if (error) {
			throw new BatchUpdateException(result);
		}
		return result;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		checkClosed();
		Command command = parseSQL(sql);
		try {
			return command.executeQuery();
		} catch (DbException e) {
			throw e.getSQLException();
		}
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		checkClosed();
		Command command = parseSQL(sql);
		try {
			int cnt =  command.executeUpdate();
			this.conn.setUpdatedInTx(true);
			return cnt;
		} catch (DbException e) {
			throw e.getSQLException();
		}
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		return executeUpdate(sql);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		return executeUpdate(sql);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		return executeUpdate(sql);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return conn;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return null;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		checkClosed();
		return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
        switch (current) {
        case Statement.CLOSE_CURRENT_RESULT:
        case Statement.CLOSE_ALL_RESULTS:
            if (resultSet != null) {
                resultSet.close();
            }
            break;
        case Statement.KEEP_CURRENT_RESULT:
            // nothing to do
            break;
        default:
            throw DbException.get(ErrorCode.INVALID_VALUE, "current:" + current);
        }
		return false;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		checkClosed();
		return resultSet;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return resultSetConcurrency;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getResultSetType() throws SQLException {
		return resultSetType;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		checkClosed();
		return updateCount;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return false;
	}

	@Override
	public void closeOnCompletion() throws SQLException {

	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return false;
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		checkClosed();
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		checkClosed();
		escapeProcessing = enable;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		checkClosed();
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		checkClosed();
	}

	@Override
	public int getFetchSize() throws SQLException {
		checkClosed();
		return fetchSize;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		checkClosed();

		try {
			if (rows < 0 || (rows > 0 && maxRows > 0 && rows > maxRows)) {
				throw DbException.getInvalidValueException("" + rows, "rows");
			}
			if (rows == 0) {
				rows = MAX_FETCH_SIZE;
			}
			fetchSize = rows;

		} catch (Exception e) {
			logAndConvert(e);
		}

	}

    @Override
	public int getMaxFieldSize() throws SQLException {
		return 0;
	}

	/**
     * Sets the maximum number of bytes for a result set column.
     * This method does currently do nothing for this driver.
     *
     * @param max the maximum size - ignored
     * @throws SQLException if this object is closed
     */
	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		checkClosed();
	}

	@Override
	public int getMaxRows() throws SQLException {
		checkClosed();
		return 0;
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		this.maxRows = max;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		// ignore
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return conn.getQueryTimeout();
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		conn.setQueryTimeout(seconds);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "isWrapperFor")
			.getSQLException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "unwrap")
			.getSQLException();
	}

	protected void checkClosed() throws SQLException{
		if(isClosed)
			DbException.get(ErrorCode.OBJECT_CLOSED, "Statemnt").getSQLException();

	}

	protected SQLException logAndConvert(Exception ex) {
		return DbException.toSQLException(ex);
	}
}
