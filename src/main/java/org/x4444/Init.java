package org.x4444;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;

public class Init {

  public static void main(String[] args) {
    ArrayList<String> qq = new ArrayList<String>();
    //qq.add("select dummy from DUAL");
    qq.add("create table if not exists DUAL (DUMMY string)");
    qq.add("add jar /usr/local/lib/hive-udf/target/nexr-hive-udf-0.2-SNAPSHOT.jar");
    //qq.add("add jar /home/nsn/lib/nexr-hive-udf-0.2-SNAPSHOT.jar");
    qq.add("CREATE TEMPORARY FUNCTION decode AS 'com.nexr.platform.hive.udf.GenericUDFDecode'");
    qq.add("select decode(1,2,3) from dual limit 1");

    Connection con = null;
    try {
      //Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      //con = DriverManager.getConnection("jdbc:hive://vmhost5-gw:10000/default",
      con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
          "alexp", "");
      Statement s = con.createStatement();
      for (String q : qq) {
        s.execute(q);
        System.out.println("prep OK: " + q);
      }
      s.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (con != null) {
        try {
          con.close();
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }
}
