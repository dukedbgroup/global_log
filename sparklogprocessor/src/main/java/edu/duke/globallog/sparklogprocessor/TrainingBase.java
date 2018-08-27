package edu.duke.globallog.sparklogprocessor;

import java.sql.*;

/*
Constants and database connection utility methods
*/
public class TrainingBase {

  // connection to database
  Connection conn = null;

  final String DB_URL = "jdbc:mysql://localhost/test";
  final String DB_USER = "root";
  final String DB_PASSWORD = "database";

  final String RELM_TABLE = "RELM";
  final String EXEC_TABLE = "EXEC_DATA";
  final String TASK_METRICS_TABLE = "TASK_METRICS_ALL";
  final String IDENTITY_TABLE = "STAGE_IDENTITY";
  final String TASK_NUMBERS_TABLE = "TASK_NUMBERS";
  final String TASK_TIMES_TABLE = "TASK_TIMES";
  final String APP_ENV_TABLE = "APP_ENV";
  final String PERF_MONITORS_TABLE = "PERF_MONITORS";

  void startConnection() {
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
      conn.setAutoCommit(false);
    } 
    catch(Exception e) {
          e.printStackTrace();
    }
  }

  void commit() {
    try {
      conn.commit();
    }
    catch(Exception e) {
          e.printStackTrace();
    }
  }

  Statement newStatement() {
    try {
      return conn.createStatement();
    }
    catch(Exception e) {
          e.printStackTrace();
    }
    return null;
  }

  PreparedStatement newPreparedStatement(String query) {
    try {
      return conn.prepareStatement(query);
    }
    catch(Exception e) {
          e.printStackTrace();
    }
    return null;
  }

  void closeStatement(Statement stmt) {
    try {
      stmt.close();
    }
    catch(Exception e) {
          e.printStackTrace();
    }
  }

  void stopConnection() {
    try {
      conn.close();
    }
    catch(Exception e) {
          e.printStackTrace();
    }
  }

}
