package com.atguigu;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQLCDC {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用FLINKSQL DDL模式构建CDC 表
        tableEnv.executeSql("CREATE TABLE user_info ( " +
                " id INT primary key, " +
                " name STRING, " +
                " age INT, " +
                " city STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'hostname' = '10.168.11.121', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'database-name' = 'cdc_test', " +
                " 'table-name' = 'user_info' " +
                ")");

/*
        tableEnv.executeSql(
            "CREATE TABLE doris_test_sink (" +
                "id INT," +
                "name STRING," +
                "age INT,\n" +
                "city STRING\n" +
                ") " +
                "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '10.168.11.122:8030',\n" +
                "  'table.identifier' = 'test_db.user_info',\n" +
                "  'sink.batch.size' = '2',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456'\n" +
                ")");
*/

        tableEnv.executeSql(
            "CREATE TABLE doris_test_sink (" +
                "id INT," +
                "name STRING," +
                "age INT,\n" +
                "city STRING\n" +
                ") " +
                "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '10.168.11.122:8030',\n" +
                "  'table.identifier' = 'test_db.user_info',\n" +
                "  'sink.batch.size' = '2',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'sink.enable-delete' = 'true'\n" +
                ")");



        //3.查询数据并转换为流输出
        //Table table = tableEnv.sqlQuery("select * from user_info");
        //DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        //retractStream.print();

        tableEnv.executeSql("INSERT INTO doris_test_sink select id,name,age,city from user_info");
        //4.启动
        env.execute("FlinkSQLCDC");

    }

}