package org.x4444;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

public class M3 {

  private boolean go = true;

  synchronized boolean isGo() {
    return go;
  }

  synchronized void setGo(boolean go) {
    this.go = go;
  }

  public static void main(String[] args) throws SQLException,
      ClassNotFoundException, InterruptedException {
    for (int j = 0; j < 1; j++) {

      M3 m1 = new M3();

      @SuppressWarnings("unchecked")
      List<MyPair<String, String>>[] res = new List[10];

      String[] q = new String[4];
      q[0] = m1.getQueryFromFile("q1.sql");
      q[1] = m1.getQueryFromFile("q2.sql");
      q[2] = m1.getQueryFromFile("q3.sql");
      q[3] = m1.getQueryFromFile("q4.sql");

      int TH = 10;
      int N = 1;
      System.out.println("TH: " + TH);
      System.out.println("N: " + N);

      Thread[] ta = new Thread[TH];
      for (int tidx = 0; tidx < TH; tidx++) {
        String name = "q-" + tidx;
        ta[tidx] = new Thread(new MyRunnable(m1, name, q, res, N));
      }

      long time = System.currentTimeMillis();

      for (Thread t : ta) {
        // Thread.sleep(1000);
        t.start();
      }

      for (Thread t : ta) {
        t.join();
      }

      if (m1.isGo()) {
        System.out.println("ALL DONE");
      } else {
        System.out.println("!!! ERROR !!!");
      }
      time = System.currentTimeMillis() - time;
      System.out.println("time taken: " + (time / 1000) + " sec");
    }
    System.out.println(System.currentTimeMillis());
  }

  static class MyRunnable implements Runnable {
    M3 m1;
    String name;
    String[] queries;
    List<MyPair<String, String>>[] ress;
    int nTimes;
    Random r = new Random();

    public MyRunnable(M3 m1, String name, String[] queries,
        List<MyPair<String, String>>[] ress, int nTimes) {
      this.m1 = m1;
      this.name = name;
      this.queries = queries;
      this.ress = ress;
      this.nTimes = nTimes;
    }

    @Override
    public void run() {
      Connection conn = null;
      try {
        System.out.println(name + " getting connection");
        conn = m1.getConnection();
        System.out.println(name + " got connection");
        for (int i = 0; i < nTimes; i++) {
          if (!m1.isGo()) {
            break;
          }

          // Thread.sleep(1000);

          if (!m1.isGo()) {
            break;
          }
          System.out.println(name + ": attempt: " + i);
          long time = System.currentTimeMillis();

          int qidx = r.nextInt(queries.length);
          String query = queries[qidx];
          List<MyPair<String, String>> res = ress[qidx];
          String qname = name + "-" + qidx;
          m1.check(qname, conn, query, res);
          time = System.currentTimeMillis() - time;
          System.out.println("== " + name + " done in " + time
              + " ms =====================");
        }
      } catch (RuntimeException e) {
        m1.setGo(false);
        throw e;
      } catch (Exception e) {
        m1.setGo(false);
        throw new RuntimeException(e);
      } finally {
        m1.close(conn);
        System.out.println(name + " connection closed");
      }
    }
  }

  Connection getConnection() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
    Connection con = DriverManager.getConnection(
        "jdbc:hive://t1:10000/default", "", "");
    // "jdbc:hive://vmhost5-gw:10000/default", "", "");
    // "jdbc:hive://vmhost10-vm5:10000/default", "", "");
    // "jdbc:hive://10.10.10.195:10000/default", "root", "");

//    ArrayList<String> qq = new ArrayList<String>();
//    qq.add("add jar /usr/local/lib/hive-udf/target/nexr-hive-udf-0.2-SNAPSHOT.jar");
//    qq.add("CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode'");
//
//    Statement s = con.createStatement();
//    for (String q : qq) {
//      s.execute(q);
//      System.out.println("prep OK: " + q);
//    }
//    close(s);

    return con;
  }

  void close(Connection conn) {
//    ArrayList<String> qq = new ArrayList<String>();
//    qq.add("drop TEMPORARY FUNCTION decode");
//
//    try {
//      Statement s = conn.createStatement();
//      for (String q : qq) {
//        s.execute(q);
//        System.out.println("drop function OK: " + q);
//      }
//      close(s);
//    } catch (Exception e) {
//      System.out.println(e.getMessage());
//    }

    if (conn != null) {
      try {
        conn.close();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  void close(Statement st) {
    if (st != null) {
      try {
        st.close();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  void check(String name, Connection conn, String query,
      List<MyPair<String, String>> res) throws SQLException {
    System.out.println(name + ": checking");
    System.out.println(query);
    PreparedStatement ps = conn.prepareStatement(query);
    try {
      ResultSet rs = ps.executeQuery();
      int colN = rs.getMetaData().getColumnCount();
      // if (colN != 2) {
      // throw new RuntimeException("not 2 columns");
      // }
      // List<MyPair<String, String>> act = new ArrayList<MyPair<String,
      // String>>();
      while (rs.next()) {
        for (int cid = 1; cid <= colN; cid++) {
          String s = rs.getString(cid);
          System.out.print(s);
          if (cid == colN) {
            System.out.println("\t");
          } else {
            System.out.print("\t");
          }
        }
      }

      // printCol(name, act);
      // boolean b = CollectionUtils.isEqualCollection(res, act);
      // System.out.println(name + ": isEqualCollection: " + b);
      // if (!b) {
      // throw new RuntimeException(name + ": not EqualCollection");
      // }
    } finally {
      close(ps);
    }
  }

  void printCol(String name, List<MyPair<String, String>> res) {
    System.out.println("-- " + name + " ------------------------------");
    for (MyPair<String, String> sa : res) {
      System.out.print(sa.t1);
      System.out.print("\t");
      System.out.println(sa.t2);
    }
    System.out.println("--------------------------------");
  }

  String getQueryFromFile(String file) {
    InputStream is = this.getClass().getResourceAsStream("/" + file);
    java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
    String query = s.hasNext() ? s.next() : "";
    if (query == null || query.trim().length() == 0) {
      throw new RuntimeException("Query is empty");
    }
    return query;
  }
}
