package com.atguigu;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import java.util.Properties;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkOracleCDC {

    public static void main(String[] args) throws Exception {

        //1.获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        setEnvProperties(env);


        //2.通过FlinkCDC构建SourceFunction
        Properties properties = new Properties();
        properties.setProperty("debezium.database.tablename.case.insensitive", "false");
        properties.setProperty("debezium.log.mining.strategy", "online_catalog");
        properties.setProperty("debezium.log.mining.continuous.mine", "true");
        properties.setProperty("scan.startup.mode", "latest-offset");

        DebeziumSourceFunction<String> sourceFunction = OracleSource.<String>builder()
            .hostname("10.168.11.138")
            .port(1521)
            .database("orcl")
            .schemaList("ODS")
            .tableList("ODS.DZ_DEPARTMENT")
            .username("ods")
            .password("oracle123")
            .debeziumProperties(properties)
            .deserializer(new JsonDebeziumDeserializationSchema())
            //只读取增量的  注意：不设置默认是先全量读取表然后增量读取日志中的变化
            .startupOptions(StartupOptions.latest())
            .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //3.数据打印
        dataStreamSource.print();
        //4.启动任务
        env.execute("FlinkOracleCDC");

    }

    private static void setEnvProperties(StreamExecutionEnvironment env)  {
        // 1. 状态后端配置
       // env.setStateBackend(new MemoryStateBackend());
        //env.setStateBackend(new FsStateBackend(""));
        // 这个需要另外导入依赖
        //env.setStateBackend(new RocksDBStateBackend("file:///rocksDb/fink-checkpoints"));
        //应用挂了的话，它默认会删除之前checkpoint数据，当然我们可以在代码中设置应用退出时保留checkpoint数据
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup
        // .RETAIN_ON_CANCELLATION);
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
