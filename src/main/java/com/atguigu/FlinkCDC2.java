package com.atguigu;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC2 {

    public static void main(String[] args) throws Exception {

        //1.获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        setEnvProperties(env);

        //1.1 开启CK
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/cdc-test/ck"));

        //2.通过FlinkCDC构建SourceFunction
       MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("10.168.11.121")
            .port(3306)
            .databaseList("cdc_test")
            .tableList("cdc_test.user_info")
            .username("root")
            .password("123456")
            .startupOptions(StartupOptions.initial())
            .debeziumProperties(getDebeziumProperties())
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        //3.数据打印
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
            .setParallelism(1)
            .print().setParallelism(1);

        //4.启动任务
        env.execute("FlinkCDC");

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
       /* env.enableCheckpointing(300);
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
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);*/

        // 3. 重启策略配置
        // 固定延迟重启(最多尝试3次，每次间隔10s)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 失败率重启(在10分钟内最多尝试3次，每次至少间隔1分钟)
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));
    }

    private static Properties getDebeziumProperties(){
        Properties properties = new Properties();
        //properties.setProperty("converters", "dateConverters");
        //根据类在那个包下面修改
/*        properties.setProperty("dateConverters.type", "com.atguigu.func.MySqlDateTimeConverter");
        properties.setProperty("dateConverters.format.date", "yyyy-MM-dd");
        properties.setProperty("dateConverters.format.time", "HH:mm:ss");
        properties.setProperty("dateConverters.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp.zone", "UTC+8");*/
        properties.setProperty("debezium.snapshot.locking.mode","none"); //全局读写锁，可能会影响在线业务，跳过锁设置
        properties.setProperty("include.schema.changes", "true");
        properties.setProperty("bigint.unsigned.handling.mode","long");
        properties.setProperty("decimal.handling.mode","double");
        return properties;
    }
}
