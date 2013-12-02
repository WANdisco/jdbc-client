package org.x4444;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class jdbcc {

  private boolean initTmpFuncForEachConnection = true;
  List<String> init_query = new ArrayList<String>();
  List<String> deinit_query = new ArrayList<String>();
  private boolean separateThreads = false;
  private boolean randomQuery = false;
  private int sleepBetween = 100000;
  private boolean verbose = false;

  private boolean go = true;
  private String connString =  "jdbc:hive://localhost:10000/default";

  String getConnString() {
    return connString;
  }

  void setConnString(String cs) {
    connString = cs;
  }

  public int getSleepBetween() {
    return sleepBetween;
  }

  public void setSleepBetween(int sleepBetween) {
    this.sleepBetween = sleepBetween;
  }

  public boolean isInitTmpFuncForEachConnection() {
    return initTmpFuncForEachConnection;
  }

  public void setInitTmpFuncForEachConnection(boolean initTmpFuncForEachConnection) {
    this.initTmpFuncForEachConnection = initTmpFuncForEachConnection;
  }

  public boolean isSeparateThreads() {
    return separateThreads;
  }

  public void setSeparateThreads(boolean separateThreads) {
    this.separateThreads = separateThreads;
  }

  public boolean isRandomQuery() {
    return randomQuery;
  }

  public void setRandomQuery(boolean randomQuery) {
    this.randomQuery = randomQuery;
  }

  synchronized boolean isGo() {
    return go;
  }

  synchronized void setGo(boolean go) {
    this.go = go;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  public static void printUsage(jdbcc jc) {
    System.err.println("Usage: jdbcc [-t <k threads>|-r|-n <n iterations>|-i <init functions query>|" +
        "-d <deinit query>|-s <time>|-p|-c <connection string>|-v] <query1 [query2...queryN]>\n");
    System.err.println("-t <k threads>               - set number of parallel threads throwing queries");
    System.err.println("-n <n iterations>            - number of iterations inside each of the threads");
    System.err.println("-i <file with init func>     - file with init queries - temp functions, etc");
    System.err.println("-d <file with deinit func>   - file with deinit queries - drop temp functions, etc");
    System.err.println("-s <timeout>                 - timeout between queries in a sequence/iterations");
    System.err.println("-c <connection string>       - Connection string to the server");
    System.err.println("-r                           - run queries in random sequence");
    System.err.println("-v                           - verbose output messages\n");

    System.err.println("Default values:\n\n\t-p 1\n\t-n 1\n\t-c " + jc.getConnString() +
                       "\n\t-s " + jc.getSleepBetween() +
                       "\n\tq1.sql q2.sql\n");
    System.exit(0);
  }

  public static void main(String[] args) throws SQLException,
      ClassNotFoundException, InterruptedException {

    int nThreads = 1;
    int nIter = 1;
    String iq=null, diq = null;

    List<String> q = new ArrayList<String>();

    jdbcc jc = new jdbcc();

    if(args.length < 1) {
      printUsage(jc);
    }

    for(int i=0; i < args.length; i++) {
      if(args[i].charAt(0) == '-') {
        switch(args[i].charAt(1)) {
          case 't':
            nThreads =  Integer.parseInt(args[++i]);
            continue;
          case 'n':
            nIter = Integer.parseInt((args[++i]));
            continue;
          case 'i':
            iq = args[++i];
            if(iq.trim().equalsIgnoreCase("none"))
              jc.setInitTmpFuncForEachConnection(false);
            else
              jc.readInitQuery(iq);
            continue;
          case 'c':
            jc.setConnString(args[++i]);
            continue;
          case 's':
            jc.setSleepBetween(Integer.parseInt(args[++i]));
            continue;
          case 'd':
            diq = args[++i];
            jc.readDeInitQuery(iq);
            continue;
//          case 'p':  // TODO: may want to execute each query in a separate thread.
//            jc.setSeparateThreads(true);
//            continue;
          case 'h':
            printUsage(jc);
            continue;
          case 'r':
            jc.setRandomQuery(true);
            continue;
          case 'v':
            jc.setVerbose(true);
            System.err.println("Verbose output: TRUE");
            continue;
          default:
            System.err.println("Unrecognized option: " + args[i]);
            continue;
        }
      }

      String s = jc.getQueryFromFile(args[i]);
      int idx;
      if((idx = s.indexOf(';')) != -1)
        s = s.substring(0, idx);
      q.add(s);
    }


    if(q.isEmpty()) {
      q.add(jc.getQueryFromFile("q1.sql"));
      q.add(jc.getQueryFromFile("q2.sql"));
    }

    if(iq == null)
      iq = "init.sql";

    System.out.println("Running queries with the following configuration:\n");
    System.out.println("\tConnString = " + jc.getConnString());
    System.out.println("\tnThreads = " + nThreads);
    System.out.println("\tnIterations = " + nIter);
    if(iq != null)
      System.out.println("\tInit SQL = " + iq);
    if(diq != null)
    System.out.println("\tDeInit SQL = " + diq);
    System.out.println("Queries: ");
    for(int i=0; i < q.size(); i++) {
      System.out.println("\t" + q.get(i));
    }
    System.out.println();

    Thread[] ta = new Thread[nThreads];
    for (int tidx = 0; tidx < nThreads; tidx++) {
      String name = "Thread[" + tidx + "]";
      ta[tidx] = new Thread(new MyRunnable(jc, name, q, nIter));
    }

    long time = System.currentTimeMillis();

    for (Thread t : ta) {
      // Thread.sleep(1000);
      t.start();
    }

    for (Thread t : ta) {
      t.join();
    }

    if (jc.isGo()) {
      System.out.println("ALL DONE");
    } else {
      System.out.println("!!! ERROR !!!");
    }
    time = System.currentTimeMillis() - time;
    System.out.println("Total time taken: " + (time / 1000) + " sec");

    System.out.println(System.currentTimeMillis());
  }


  static class MyRunnable implements Runnable {
    jdbcc jc;
    String name;
    List<String> queries;
    List<MyPair<String, String>>[] ress;
    int nTimes;
    Random r = new Random();
    int cur_num = 0;

    public MyRunnable(jdbcc jc, String name, List<String> queries, int nTimes) {
      this.jc = jc;
      this.name = name;
      this.queries = queries;
      this.ress = new List[10];
      this.nTimes = nTimes;
    }

    private int getNextIdx()
    {
      int qidx;
      if(jc.isRandomQuery()) {
        qidx = r.nextInt(queries.size());
      } else {
        if(++cur_num > queries.size()-1)
          cur_num = 0;
        qidx = cur_num;
      }
      return qidx;
    }


    @Override
    public void run() {
      Connection conn = null;
      try {
        System.out.println(name + ": getting connection");
        conn = jc.getConnection(name);
        System.out.println(name + ": got connection");

        for (int i = 0; i < nTimes; i++) {
          if (!jc.isGo()) {
            break;
          }
          
          if (i > 1) {
          	Thread.sleep(jc.getSleepBetween());
          }

          if (!jc.isGo()) {
            break;
          }
          System.out.println(name + ": attempt: " + i);
          long time = System.currentTimeMillis();

          int qidx = getNextIdx();
          String query = queries.get(qidx);
          List<MyPair<String, String>> res = ress[qidx];
          jc.check(name, qidx, conn, query, res);
          time = System.currentTimeMillis() - time;
          System.out.println(name + ": == DONE (" + i + ", " + qidx + ") in " + time
              + " ms =====================");
        }
      } catch (RuntimeException e) {
        jc.setGo(false);
        throw e;
      } catch (Exception e) {
        jc.setGo(false);
        throw new RuntimeException(e);
      } finally {
        jc.close(conn, name);
        System.out.println(name + ": connection closed");
      }
    }
  }

  Connection getConnection(String name) throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
    Connection con = DriverManager.getConnection(connString, "", "");
    if(verbose)
      System.err.println(name + ": Connection opened, initing temp function...");

    if (isInitTmpFuncForEachConnection()) {
      createTmpFunc(con, name);
    }

    if(verbose)
      System.err.println(name + ": temp functions initialized");

    return con;
  }

  void close(Connection conn, String name) {
    if (isInitTmpFuncForEachConnection()) {
      dropTmpFunc(conn, name);
    }

    if (conn != null) {
      try {
        conn.close();
      } catch (Exception e) {
        // ignore
      }
    }
  }



  private List<String> getFuncFromFile(String file)
  {
    List<String> qq = new ArrayList<String>();
    try {
      BufferedReader rdr = new BufferedReader(new FileReader(file));
      String line;
      int idx;
      while( (line = rdr.readLine()) != null) {
        line = line.trim();
        if(line.isEmpty() || line.startsWith("#"))
          continue;
        if((idx = line.indexOf(';'))!= -1)
          line = line.substring(0, idx);
        qq.add(line);
      }
      rdr.close();
    } catch(Exception e) {
      System.err.println("Can not read init file: " + file + " : " + e.getMessage());
      if(verbose) {
        e.printStackTrace();
      }
      return null;
    }

    if(qq.isEmpty())
      return null;

    return qq;
  }

  public void readInitQuery(String iq)
  {
    List<String> qq = getFuncFromFile(iq);
    if(qq != null) {
      init_query = qq;
      if(verbose)
        System.err.println("Got init query from file: " + iq);
    } else {
        init_query.add("add jar /usr/local/lib/hive-udf/target/nexr-hive-udf-0.2-SNAPSHOT.jar");
        init_query.add("CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode'");
      if(verbose)
        System.err.println("Using default init query");
    }
  }

  public void readDeInitQuery(String iq)
  {
    List<String> qq = getFuncFromFile(iq);
    if(qq != null) {
      deinit_query = qq;
    } else {
      deinit_query.add("drop TEMPORARY FUNCTION decode");
    }
  }


  void createTmpFunc(Connection con, String name) throws SQLException {

    Statement s = con.createStatement();
    if(verbose)
      System.err.println(name + ": tmpFunc - statement created");

    try {
      for (String q : init_query) {
        s.execute(q);
      }
      System.out.println(name + ": init functions OK");
    } catch(Exception e) {
      System.err.println(name + ": Got exception for init functions: "+ e.getMessage());
      if(verbose)
        e.printStackTrace();
      throw new RuntimeException(e.getMessage());
    } finally {
      close(s);
    }
  }

  void dropTmpFunc(Connection con, String name) {
    try {
      Statement s = con.createStatement();
      try {
        for (String q : deinit_query) {
          s.execute(q);
        }
        System.out.println(name + ": drop function OK");
      } finally {
        close(s);
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  void close(Statement st) {
    if (st != null) {
      try {
        st.close();
        System.out.println("Closed Statement");
      } catch (Exception e) {
      	System.out.println("Failed to close statement!");
      	e.printStackTrace();
        // ignore
      }
    } else {
    	System.out.println("Null Statement");
    }
  }

  void check(String name, int idx, Connection conn, String query, List<MyPair<String, String>> res) {
    System.out.println(name + ": running query #" + idx);
    System.out.println(name + ": " + query);
    System.out.println();
    PreparedStatement ps = null;
    try {
      if(verbose)
        System.err.println(name + ": preparing statement from the query...");
      ps = conn.prepareStatement(query);
      if(verbose) {
        System.err.println(name + ": statement prepared");
        System.err.println(name + ": Running query...");
      }

      ResultSet rs = ps.executeQuery();

      if(verbose)
        System.err.println(name + ": Query completed... getting results");

      int colN = rs.getMetaData().getColumnCount();
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
      rs.close();

      if(verbose)
        System.err.println(name + ": Got results.");
    } catch(Exception e) {
      System.err.println(name + ": Exception while running query #" + idx + " : " + e.getMessage());
      if(verbose)
        e.printStackTrace();
    } finally {
      if(ps != null)
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

  String getQueryFromFile(String file)
  {
    String query = "";
    try {
      BufferedReader rdr = new BufferedReader(new FileReader(file));
      String line;
      while( (line = rdr.readLine()) != null) {
        line = line.trim();
        if(line.isEmpty() || line.startsWith("#"))
          continue;
        query += " " + line;
      }
      rdr.close();
    } catch(Exception e) {
      throw new RuntimeException("Can not open query: " + file + " : " + e.getMessage());
    }
    if(query.trim().isEmpty())
      throw new RuntimeException("Query " + file + " is empty");
    return query.trim();
  }
}
