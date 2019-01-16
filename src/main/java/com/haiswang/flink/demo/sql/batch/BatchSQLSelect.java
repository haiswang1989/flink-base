package com.haiswang.flink.demo.sql.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Batch SQL
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月15日 上午10:14:42
 */
public class BatchSQLSelect {

    public static void main(String[] args) throws Exception {
        
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
        DataSet<Data> csvInput = env.readCsvFile("E:\\udp_health_degree_inner_stat.csv").ignoreFirstLine()
                .pojoType(Data.class, "user_id", "hd_net_profit_minus", "orders_cnt", "goods_refuse_rate", "hd_vip_risk_user", 
                        "hd_vip_wms_bas", "hd_vip_stat_user", "hd_svip_handle", "hd_vip_gift_ref", "hd_vip_gift_ref_cnt",
                        "value_contribution_score_last", "historical_performance_score_last", "integrity_perform_score_last", "dt");
        
        Table testData = tableEnv.fromDataSet(csvInput);
        tableEnv.registerTable("testdata", testData);
        
        //
        Table countResult = tableEnv.sqlQuery("select count(1) from testdata");
        DataSet<Long> result = tableEnv.toDataSet(countResult, Long.class);
        result.print();
        
        //注意这边select sql的column和下面的Result对象的字段名要对上
        //否则会报错"org.apache.flink.table.api.TableException:POJO does not define field name: EXPR$1"
        //EXPR$0就表示第一个字段
        //EXPR$1就表示第二个字段
        Table groupByCountResult = tableEnv.sqlQuery("select hd_svip_handle, count(1) as resultCount from testdata group by hd_svip_handle");
        DataSet<GroupByCountResult> groupByResult = tableEnv.toDataSet(groupByCountResult, GroupByCountResult.class);
        groupByResult.map(new MapFunction<GroupByCountResult, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String map(GroupByCountResult result) throws Exception {
                return result.getHd_svip_handle() + " : " + result.getResultCount();
            }
        }).print();
    }
}
