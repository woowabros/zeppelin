/*
  Licensed to the Apache Software Foundation (ASF) under one or more contributor license
  agreements. See the NOTICE file distributed with this work for additional information regarding
  copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance with the License. You may obtain a
  copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software distributed under the License
  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
  or implied. See the License for the specific language governing permissions and limitations under
  the License.
 */

package org.apache.zeppelin.presto;

import com.facebook.presto.client.*;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Presto interpreter for Zeppelin.
 */
public class PrestoInterpreter extends Interpreter {
  private static final Logger logger = LoggerFactory.getLogger(PrestoInterpreter.class);

  private static final String CAUTION_LOGO =
          " ██████╗ █████╗ ██╗   ██╗████████╗██╗ ██████╗ ███╗   ██╗    ██╗    \n" +
          "██╔════╝██╔══██╗██║   ██║╚══██╔══╝██║██╔═══██╗████╗  ██║    ██║    \n" +
          "██║     ███████║██║   ██║   ██║   ██║██║   ██║██╔██╗ ██║    ██║    \n" +
          "██║     ██╔══██║██║   ██║   ██║   ██║██║   ██║██║╚██╗██║    ╚═╝    \n" +
          "╚██████╗██║  ██║╚██████╔╝   ██║   ██║╚██████╔╝██║ ╚████║    ██╗    \n" +
          " ╚═════╝╚═╝  ╚═╝ ╚═════╝    ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝    ╚═╝";

  private static final String PRESTOSERVER_URL = "presto.url";
  private static final String PRESTOSERVER_CATALOG = "presto.catalog";
  private static final String PRESTOSERVER_SCHEMA = "presto.schema";
  private static final String PRESTOSERVER_USER = "presto.user";
  private static final String PRESTOSERVER_PASSWORD = "presto.password";
  private static final String PRESTOSERVER_SOURCE_PREFIX = "presto.source.prefix";
  private static final String PRESTOSERVER_TIMEZONE = "presto.timezone";
  private static final String PRESTO_MAX_RESULT_ROW = "presto.notebook.rows.max";
  private static final String PRESTO_MAX_ROW = "presto.rows.max";
  private static final String PRESTO_RESULT_PATH = "presto.result.path";
  private static final String PRESTO_RESULT_EXPIRE_SECONDS = "presto.result.expire.sec";
  private static final String PRESTO_HIGHLIGHT_LIMIT = "presto.highlight_limit";

  static final String LIMIT_QUERY_HEAD = "SELECT * FROM (\n";
  static final String LIMIT_QUERY_TAIL = "\n) ORIGINAL \nLIMIT ";
  static final int DEFAULT_LIMIT_ROW = 100000;

  private int maxRowsinNotebook = 1000;
  private int maxLimitRow = DEFAULT_LIMIT_ROW;
  private String resultDataDir;
  private long expireResult;
  private String prestoUser;
  private String prestoSourcePrefix;
  private String timezone;
  private boolean highlightLimit;

  private OkHttpClient httpClient;
  private final Map<String, ClientSession> prestoSessions = new HashMap<>();
  private Exception exceptionOnConnect;
  private URI prestoServer;
  private CleanResultFileThread cleanThread;

  private final Map<String, ParagraphTask> paragraphTasks = new HashMap<>();

  public PrestoInterpreter(Properties property) {
    super(property);
  }

  private class ParagraphTask {
    StatementClient sqlStatement;
    QueryData sqlQueryData;

    AtomicBoolean reportProgress = new AtomicBoolean(false);
    AtomicBoolean queryCanceled = new AtomicBoolean(false);
    long timestamp;

    public ParagraphTask() {
      this.timestamp = System.currentTimeMillis();
    }

    public void setQueryData(QueryData queryData) {
      sqlQueryData = queryData;
    }

    public synchronized void close() {
      reportProgress.set(false);
      queryCanceled.set(true);
      if (sqlStatement != null) {
        try {
          sqlStatement.close();
        } catch (Exception ignored) {
        }
      }
      sqlStatement = null;
    }

    public String getQueryResultId() {
      if (sqlQueryData != null) {
        return sqlStatement.currentStatusInfo().getId();
      } else {
        return null;
      }
    }
  }

  class CleanResultFileThread extends Thread {
    @Override
    public void run() {
      long twoHour = 2 * 60 * 60 * 1000;
      logger.info("Presto result file cleaner started.");
      while (true) {
        try {
          Thread.sleep(10 * 60 * 1000);
        } catch (InterruptedException e) {
          break;
        }
        long currentTime = System.currentTimeMillis();

        List<String> expiredParagraphIds = new ArrayList<>();
        synchronized (paragraphTasks) {
          for (Map.Entry<String, ParagraphTask> entry : paragraphTasks.entrySet()) {
            ParagraphTask task = entry.getValue();
            if (currentTime - task.timestamp > twoHour) {
              task.close();
              expiredParagraphIds.add(entry.getKey());
            }
          }

          for (String paragraphId : expiredParagraphIds) {
            paragraphTasks.remove(paragraphId);
          }
        }

        try {
          File file = new File(resultDataDir);
          if (!file.exists()) {
            continue;
          }
          if (!file.isDirectory()) {
            logger.error(file + " is not directory.");
            continue;
          }

          File[] files = file.listFiles();
          if (files != null) {
            for (File eachFile: files) {
              if (eachFile.isDirectory()) {
                continue;
              }

              if (currentTime - eachFile.lastModified() >= expireResult) {
                logger.info("Delete " + eachFile + " because of expired.");
                eachFile.delete();
              }
            }
          }
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        }
      }
      logger.info("Presto result file cleaner stopped.");
    }
  }

  @Override
  public void open() {
    logger.info("Presto interpreter open called!");

    prestoUser = getProperty(PRESTOSERVER_USER);

    try {
      String timezoneProperty = getProperty(PRESTOSERVER_TIMEZONE);
      if (timezoneProperty != null && !timezoneProperty.equals("")) {
        timezone = TimeZone.getTimeZone(timezoneProperty).getID();
      } else {
        timezone = TimeZone.getDefault().getID();
      }

      String maxRowsProperty = getProperty(PRESTO_MAX_RESULT_ROW);
      if (maxRowsProperty != null) {
        try {
          maxRowsinNotebook = Integer.parseInt(maxRowsProperty);
        } catch (Exception e) {
          logger.error("presto.notebook.rows.max property error: " + e.getMessage());
          maxRowsinNotebook = 1000;
        }
      }

      String maxLimitRowsProperty = getProperty(PRESTO_MAX_ROW);
      if (maxLimitRowsProperty != null) {
        try {
          maxLimitRow = Integer.parseInt(maxLimitRowsProperty);
        } catch (Exception e) {
          logger.error("presto.rows.max property error: " + e.getMessage());
          maxLimitRow = DEFAULT_LIMIT_ROW;
        }
      }

      String expireResultProperty = getProperty(PRESTO_RESULT_EXPIRE_SECONDS);
      if (expireResultProperty != null) {
        try {
          expireResult = Integer.parseInt(expireResultProperty) * 1000;
        } catch (Exception e) {
          expireResult = 2 * 60 * 60 * 24 * 1000;
        }
      }

      prestoSourcePrefix = getProperty(PRESTOSERVER_SOURCE_PREFIX);
      if (prestoSourcePrefix == null) {
        prestoSourcePrefix = "zeppelin-";
      }

      String highlightLimitProperty = getProperty(PRESTO_HIGHLIGHT_LIMIT);
      if (highlightLimitProperty != null && !highlightLimitProperty.equals("")) {
        highlightLimit = Boolean.valueOf(highlightLimitProperty);
      } else {
        highlightLimit = true;
      }

      resultDataDir = getProperty(PRESTO_RESULT_PATH);
      if (resultDataDir == null) {
        resultDataDir = "/tmp/zeppelin-" + System.getProperty("user.name");
      }

      File file = new File(resultDataDir);
      if (!file.exists()) {
        if (!file.mkdir()){
          logger.error("Can't make result directory: " + file);
        } else {
          logger.info("Created result directory: " + file);
        }
      }

      prestoServer =  new URI(getProperty(PRESTOSERVER_URL));
      OkHttpClient.Builder builder = new OkHttpClient.Builder();
      httpClient = builder.build();

      cleanThread = new CleanResultFileThread();
      cleanThread.start();
      logger.info("Presto interpreter is opened!");
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      exceptionOnConnect = e;
    }
  }

  private ClientSession getClientSession(String userId) {
    synchronized (prestoSessions) {
      ClientSession prestoSession = prestoSessions.get(userId);
      if (prestoSession == null) {
        String user = (prestoUser != null && prestoUser.length() > 0) ? prestoUser : userId;
        prestoSession = new ClientSession(
            prestoServer,
            user,
            prestoSourcePrefix + userId,
            Collections.singleton("zeppelin-presto-interpreter"),
            "Zeppelin Presto Interpreter",
            getProperty(PRESTOSERVER_CATALOG),
            getProperty(PRESTOSERVER_SCHEMA),
            timezone,
            Locale.getDefault(),
            Collections.<String, String>emptyMap(),
            Collections.<String, String>emptyMap(),
            Collections.<String, String>emptyMap(),
            null,
            new Duration(10, TimeUnit.SECONDS));

        prestoSessions.put(userId, prestoSession);
      }
      return prestoSession;
    }
  }

  @Override
  public void close() {
    try {
      cleanThread.interrupt();
    } finally {
      httpClient = null;
      exceptionOnConnect = null;
      cleanThread = null;
    }

    synchronized (paragraphTasks) {
      for (ParagraphTask task: paragraphTasks.values()) {
        task.close();
      }
      paragraphTasks.clear();
    }
  }

  private ParagraphTask getParagraphTask(InterpreterContext context) {
    synchronized (paragraphTasks) {
      ParagraphTask task = paragraphTasks.get(context.getParagraphId());
      if (task == null) {
        task = new ParagraphTask();
        paragraphTasks.put(context.getParagraphId(), task);
      }

      return task;
    }
  }

  private void removeParagraph(InterpreterContext context) {
    synchronized (paragraphTasks) {
      paragraphTasks.remove(context.getParagraphId());
    }
  }

  private InterpreterResult executeSql(String sql,
                                       InterpreterContext context) {
    String sqlForTest = convertSqlForTest(sql);

    boolean isSelectSql = sqlForTest.startsWith("select") || sqlForTest.startsWith("with");

    InterpreterOutput interpreterOutput = context.out();
    ResultFileMeta resultFileMeta = null;
    ParagraphTask task = getParagraphTask(context);
    try {
      if (sql == null || sql.trim().isEmpty()) {
        return new InterpreterResult(Code.ERROR, "No query");
      }

      QueryProcessResult queryProcessResult;
      if (isSelectSql) {
        queryProcessResult = addLimitClause(sql);
      } else {
        queryProcessResult = new QueryProcessResult(sql, "");
      }

      if (queryProcessResult.getMessage() != null && !queryProcessResult.getMessage().equals(""))
        writeToInterpreterOutput(interpreterOutput, queryProcessResult.getMessage());

      if (exceptionOnConnect != null) {
        return new InterpreterResult(Code.ERROR, exceptionOnConnect.getMessage());
      }
      ClientSession clientSession = getClientSession(context.getAuthenticationInfo().getUser());
      StatementClient statementClient =
              StatementClientFactory.newStatementClient(httpClient, clientSession,
                      buildQueryComment(context) + "\n" + queryProcessResult.getQuery());

      task.sqlStatement = statementClient;
      StringBuilder msg = new StringBuilder();

      boolean alreadyPutColumnName = false;
      AtomicInteger receivedRows = new AtomicInteger(0);

      String resultFilePath =
          resultDataDir + "/" + context.getNoteId() + "_" + context.getParagraphId();
      File resultFile = new File(resultFilePath);
      if (resultFile.exists()) {
        resultFile.delete();
      }
      while (statementClient.isRunning()
          && statementClient.advance()) {
        QueryData queryData = statementClient.currentData();
        task.setQueryData(queryData);

        if (!task.reportProgress.get()) {
          task.reportProgress.set(true);
        }
        Iterable<List<Object>> data  = queryData.getData();
        if (data == null) {
          continue;
        }
        if (!alreadyPutColumnName) {
          List<Column> columns = statementClient.currentStatusInfo().getColumns();
          String prefix = "";
          for (Column eachColumn: columns) {
            msg.append(prefix).append(eachColumn.getName());
            if (prefix.isEmpty()) {
              prefix = "\t";
            }
          }
          msg.append("\n");
          alreadyPutColumnName = true;
        }

        resultFileMeta = processData(context, data, receivedRows,
            isSelectSql, msg, resultFileMeta);
      }
      if (statementClient.finalStatusInfo().getError() != null) {
        return new InterpreterResult(Code.ERROR, statementClient.finalStatusInfo().getError().getMessage());
      }
      if (resultFileMeta != null) {
        resultFileMeta.outStream.close();
      }

      InterpreterResult result = new InterpreterResult(Code.SUCCESS,
          StringUtils.containsIgnoreCase(queryProcessResult.getQuery(), "EXPLAIN ") ? msg.toString() :
              "%table " + msg.toString());
      return result;
    } catch (Exception ex) {
      ex.printStackTrace();
      logger.error("Can not run " + sql, ex);

      StringWriter errorStringWriter = new StringWriter();
      PrintWriter errorPrintWriter = new PrintWriter(errorStringWriter);
      ex.printStackTrace(errorPrintWriter);

      return new InterpreterResult(Code.ERROR, errorStringWriter.toString());
    } finally {
      if (resultFileMeta != null && resultFileMeta.outStream != null) {
        try {
          resultFileMeta.outStream.close();
        } catch (IOException ignored) {
        }
      }
    }
  }

  private static String buildQueryComment(InterpreterContext context) {
    String userId = "";
    if (context.getAuthenticationInfo() != null) {
      userId = getUserName(context.getAuthenticationInfo());
    }

    return "/* From: Zeppelin" +
            ", UserID: " + userId +
            ", NoteID: " + context.getNoteId() +
            ", ParagraphID: " + context.getParagraphId() + " */";
  }

  private static String getUserName(AuthenticationInfo info) {
    String uName = "";
    if (info != null) {
      uName = info.getUser();
    }
    if (uName == null || uName.isEmpty()) {
      uName = "anonymous";
    }
    return uName;
  }

  private String resultToCsv(String resultMessage) {
    StringBuilder sb = new StringBuilder();
    String[] lines = resultMessage.split("\n");

    for (String eachLine: lines) {
      String[] tokens = eachLine.split("\t");
      String prefix = "";
      for (String eachToken: tokens) {
        sb.append(prefix).append("\"").append(eachToken.replace("\"", "'")).append("\"");
        prefix = ",";
      }
      sb.append("\n");
    }

    return sb.toString();
  }

  private ResultFileMeta processData(
      InterpreterContext context,
      Iterable<List<Object>> data,
      AtomicInteger receivedRows,
      boolean isSelectSql,
      StringBuilder msg,
      ResultFileMeta resultFileMeta) throws IOException {
    for (List<Object> row : data) {
      receivedRows.incrementAndGet();
      if (receivedRows.get() > maxRowsinNotebook && resultFileMeta == null) {
        resultFileMeta = new ResultFileMeta();
        resultFileMeta.filePath =
            resultDataDir + "/" + context.getNoteId() + "_" + context.getParagraphId();

        resultFileMeta.outStream = new FileOutputStream(resultFileMeta.filePath);
        resultFileMeta.outStream.write(0xEF);
        resultFileMeta.outStream.write(0xBB);
        resultFileMeta.outStream.write(0xBF);
        resultFileMeta.outStream.write(resultToCsv(msg.toString()).getBytes("UTF-8"));
      }
      String delimiter = "";
      String csvDelimiter = "";
      for (Object col : row) {
        String colStr =
            (col == null ? "null" :
                col.toString().replace('\n', ' ').replace('\r', ' ')
                    .replace('\t', ' ').replace('\"', '\''));
        if (receivedRows.get() > maxRowsinNotebook) {
          resultFileMeta.outStream.write((csvDelimiter + "\"" + colStr + "\"").getBytes("UTF-8"));
        } else {
          msg.append(delimiter).append(!isSelectSql ? col.toString() : colStr);
        }

        if (delimiter.isEmpty()) {
          delimiter = "\t";
          csvDelimiter = ",";
        }
      }
      if (receivedRows.get() > maxRowsinNotebook) {
        resultFileMeta.outStream.write(("\n").getBytes());
      } else {
        msg.append("\n");
      }
    }
    return resultFileMeta;
  }

  QueryProcessResult addLimitClause(String sql) {
    String sqlForTest = convertSqlForTest(sql);

    if (!sqlForTest.contains("from")) {
      return new QueryProcessResult(sql, "");
    }

    String[] tokens = sqlForTest.replaceAll(" +", " ")
            .split(" ");

    int limitLocation = getLastLimitTokenLocation(tokens);

    if (limitLocation == -1) {
      String cautionLogo = "";
      if (highlightLimit) {
        cautionLogo = CAUTION_LOGO;
      }

      return new QueryProcessResult(LIMIT_QUERY_HEAD + sql + LIMIT_QUERY_TAIL + maxLimitRow,
              cautionLogo + "\n\n" +
                      "No limit clause!\n" +
                      "Please Add 'LIMIT' in Your Query!\n" +
                      "('LIMIT " + maxLimitRow + "' is Applied)");
    }

    if (limitLocation == tokens.length - 1) {
      throw new RuntimeException("Query is not completed: " + sql);
    }

    int limitValue;
    try {
      limitValue = Integer.parseInt(tokens[limitLocation + 1].trim());
    } catch (Exception e) {
      throw new RuntimeException("Query Error: " + sql);
    }

    if (limitValue > maxLimitRow) {
      return new QueryProcessResult(LIMIT_QUERY_HEAD + sql + LIMIT_QUERY_TAIL + maxLimitRow,
              "Limit clause exceeds " + maxLimitRow);
    } else {
      return new QueryProcessResult(sql, "");
    }
  }

  private String convertSqlForTest(String source) {
    String convertedSql = source.trim().toLowerCase();

    convertedSql = removeCommentInSql(convertedSql);
    convertedSql = removeWhiteSpaceCharacter(convertedSql);

    return convertedSql.trim();
  }

  private String removeCommentInSql(String source) {
    StringBuilder result = new StringBuilder();
    String commentType1RemovedSource = source
            .replaceAll("/\\*(?:.|[\\r\\n])*?\\*/", "");

    String[] splitCommentType1RemovedSource = commentType1RemovedSource.split("\n");
    for (String line: splitCommentType1RemovedSource) {
      result.append(line.replaceAll("--.*$", "")).append("\n");
    }

    return result.toString();
  }

  private String removeWhiteSpaceCharacter(String source) {
    return source
            .replace('\n', ' ')
            .replace('\r', ' ')
            .replace('\t', ' ')
            ;
  }

  private int getLastLimitTokenLocation(String[] tokens) {
    int limitLocation = -1;
    for (int i = 0; i < tokens.length; ++i) {
      if (tokens[i].equals("limit")) {
        limitLocation = i;
      }
    }
    return limitLocation;
  }


  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext context) {
    AuthenticationInfo authInfo = context.getAuthenticationInfo();
    String user = authInfo == null ? "anonymous" : authInfo.getUser();
    logger.info("Run SQL command user['" + user + "'], [" + cmd + "]");
    return executeSql(cmd, context);
  }

  @Override
  public void cancel(InterpreterContext context) {
    ParagraphTask task = getParagraphTask(context);
    try {
      if (task.sqlStatement == null) {
        return;
      }

      logger.info("Kill query '" + task.getQueryResultId() + "'");

      task.sqlStatement.close();
    } finally {
      removeParagraph(context);
    }
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    ParagraphTask task = getParagraphTask(context);
    if (!task.reportProgress.get() || task.sqlQueryData == null) {
      return 0;
    }
    StatementStats stats = task.sqlStatement.getStats();
    if (stats.getTotalSplits() == 0) {
      return 0;
    } else {
      double p = (double) stats.getCompletedSplits() / (double) stats.getTotalSplits();
      return (int) (p * 100.0);
    }
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
        PrestoInterpreter.class.getName() + this.hashCode(), 5);
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    return null;
  }

  static class ResultFileMeta {
    String filePath;
    OutputStream outStream;
  }

  private void writeToInterpreterOutput(InterpreterOutput interpreterOutput, String message) {
    try {
      interpreterOutput.write(message + "\n");
    } catch (Exception e) {
      logger.warn("InterpreterOutput Exception", e);
    }
  }

  static class QueryProcessResult {
    private String query;
    private String message;

    public QueryProcessResult(String query, String message) {
      this.query = query;
      this.message = message;
    }

    public String getQuery() {
      return query;
    }

    public String getMessage() {
      return message;
    }
  }
}
