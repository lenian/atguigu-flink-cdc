package com.atguigu.oracle;

/**
 * 同步oracle指定表 发送到kafka
 *
 * @author gym
 * @version v1.0
 * @description:
 * @date: 2022/3/31 14:25
 */


import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class OracleToKafka {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("debezium.database.tablename.case.insensitive", "false");
        properties.setProperty("debezium.log.mining.strategy", "online_catalog");
        properties.setProperty("debezium.log.mining.continuous.mine", "true");
        properties.setProperty("scan.startup.mode", "latest-offset");
        //properties.setProperty("debezium.snapshot.mode", "latest-offset");
        String user = LocalFileConfigParam.getPropertiesString("dataSource.user", "ods");
        String password = LocalFileConfigParam.getPropertiesString("dataSource.password", "oracle123");
        String tableStr = LocalFileConfigParam.getPropertiesString("monitor.tableList", "OTHERTOODS_MAPPING,DZ_DEPARTMENT,DZ_EMPLOYEE");
        String host = LocalFileConfigParam.getPropertiesString("dataSource.host", "10.168.11.138");
        Integer port = LocalFileConfigParam.getPropertiesInt("dataSource.port", 1521);
        String serviceName = LocalFileConfigParam.getPropertiesString("dataSource.serviceName", "ORCL");
        String[] tableArr;
        if (tableStr.indexOf(",") > 0) {
            tableArr = Arrays.stream(tableStr.split(",")).map(s -> user + "." + s).toArray(String[]::new);
        } else {
            tableArr = new String[]{user + "." + tableStr};
        }
        DebeziumSourceFunction<String> sourceFunction = OracleSource.<String>builder()
                .hostname(host)
                .port(port)
                .database(serviceName) // monitor XE database
                .schemaList(user) // monitor inventory schema   NH_CJ_DYDLQX,NH_CJ_DNSZQX,NH_CJ_GLQX
                .tableList(tableArr) // monitor products table
                .username(user)
                .password(password)
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                //只读取增量的  注意：不设置默认是先全量读取表然后增量读取日志中的变化
                .startupOptions(StartupOptions.latest())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setEnvProperties(env);

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction).setParallelism(1);
        dataStreamSource.print();
        /*SingleOutputStreamOperator<Tuple2<String, String>> streamOperator = dataStreamSource.filter(s -> StringUtils.isNotEmpty(s)).map(new MapFunction<String, OracleDataObj>() {
                    @Override
                    public OracleDataObj map(String s) throws Exception {
                        //转换数据
                        return JSONObject.parseObject(s, OracleDataObj.class);
                    }
                }).filter(s -> MapUtils.isEmpty(s.getBefore()))   //过滤掉非insert操作
                .map(new MapFunction<OracleDataObj, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(OracleDataObj oracleDataObj) throws Exception {
                        //封装topic和数据
                        String tableName = MapUtils.getString(oracleDataObj.getSource(), "table");
                        String jsonString = JSONObject.toJSONString(oracleDataObj.getAfter());
                        return Tuple2.of("NH_" + tableName, jsonString);
                    }
                });
        streamOperator.setParallelism(1).print();*/
        //发送到kafka
        //streamOperator.addSink(new KafkaSink());
        env.execute("FlinkCDCOracle");
    }

    private static void setEnvProperties(StreamExecutionEnvironment env) throws IOException {
        // 1. 状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        //env.setStateBackend(new FsStateBackend(""));
        // 这个需要另外导入依赖
        //env.setStateBackend(new RocksDBStateBackend("file:///rocksDb/fink-checkpoints"));
        //应用挂了的话，它默认会删除之前checkpoint数据，当然我们可以在代码中设置应用退出时保留checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2. 检查点配置 (每300ms让jobManager进行一次checkpoint检查)
        env.enableCheckpointing(300);
        // 高级选项
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //Checkpoint的处理超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 最大允许同时处理几个Checkpoint(比如上一个处理到一半，这里又收到一个待处理的Checkpoint事件)
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 与上面setMaxConcurrentCheckpoints(2) 冲突，这个时间间隔是 当前checkpoint的处理完成时间与接收最新一个checkpoint之间的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        // 如果同时开启了savepoint且有更新的备份，是否倾向于使用更老的自动备份checkpoint来恢复，默认false
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 最多能容忍几次checkpoint处理失败（默认0，即checkpoint处理失败，就当作程序执行异常）
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        // 3. 重启策略配置
        // 固定延迟重启(最多尝试3次，每次间隔10s)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 失败率重启(在10分钟内最多尝试3次，每次至少间隔1分钟)
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));
    }
}

