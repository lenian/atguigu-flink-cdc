package com.atguigu;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
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

        tableEnv.executeSql("CREATE TABLE `user_info2`(\n"
            + "    `id` INT,\n"
            + "    `name` STRING,\n"
            + "    `age` INT,\n"
            + "    `city` STRING\n"
            + "    ,PRIMARY KEY (\n"
            + "        `id`\n"
            + "    ) NOT ENFORCED\n"
            + ")\n"
            + "with (\n"
            + "    'password' = '123456',\n"
            + "    'connector' = 'jdbc',\n"
            + "    'sink.max-retries' = '2',\n"
            + "    'sink.buffer-flush.interval' = '60',\n"
            + "    'sink.buffer-flush.max-rows' = '100',\n"
            + "    'table-name' = 'user_info2',\n"
            + "    'url' = 'jdbc:mysql://10.168.11.121:3306/dong_test?autoReconnect=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai',\n"
            + "    'username' = 'root'\n"
            + ")");

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

        /*tableEnv.executeSql(
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
                ")");*/

        //3.查询数据并转换为流输出
       /* Table table = tableEnv.sqlQuery("select * from user_info");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();*/

       //sink到mysql, 同一主键的增删改都同步到sink端
        tableEnv.executeSql("INSERT INTO user_info2 select id,name,age,city from user_info");
        //4.启动
        env.execute("FlinkSQLCDC");

    }

}
