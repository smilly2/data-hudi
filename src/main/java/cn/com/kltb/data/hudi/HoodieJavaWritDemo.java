package cn.com.kltb.data.hudi;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.*;


/**
 * Simple examples of #{@link HoodieJavaWriteClient}.
 * <p>
 * Usage: HoodieJavaWriteClientExample <tablePath> <tableName>
 * <tablePath> and <tableName> describe root path of hudi and table name
 * for example, `HoodieJavaWriteClientExample file:///tmp/hoodie/sample-table hoodie_rt`
 */
public class HoodieJavaWritDemo {
    public static String PERSON_SCHEMA = "{\"type\":\"record\",\"name\":\"personrec\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"job\",\"type\":\"string\"},{\"name\":\"salary\",\"type\":\"double\"}]}";
    public static Schema avroSchema = new Schema.Parser().parse(PERSON_SCHEMA);

    private static final Logger LOG = LogManager.getLogger(HoodieJavaWritDemo.class);

    private static String tableType = HoodieTableType.COPY_ON_WRITE.name();

    public static void main(String[] args) throws Exception {

        String tablePath = "/hudi/tmp/t_personx";
        String tableName = "t_personx";

        // initialize the table, if not done already
        Path path = new Path(tablePath);
        FileSystem fs = FSUtils.getFs(tablePath, Config.hadoopConf);
        if (!fs.exists(path)) {
            // 使用自定义配置定义hudi 表配置
            final Properties properties = new Properties();
            // 指定非分区表
            properties.setProperty(KEY_GENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
            properties.setProperty(PARTITION_FIELDS.key(), "");

            properties.setProperty(RECORDKEY_FIELDS.key(), "id");
            // 指定没有pre combine字段
            properties.setProperty(PRECOMBINE_FIELD.key(), "");
            // 初始化表
            HoodieTableMetaClient.withPropertyBuilder()
                    .fromProperties(properties)
                    .setTableType(tableType)
                    .setTableType(HoodieTableType.COPY_ON_WRITE)
                    .setTableName(tableName)
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .initTable(Config.hadoopConf, tablePath);
        }

        // 创建write client用于写入数据
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withSchema(PERSON_SCHEMA)
                //.withParallelism(2, 2)
                //.withDeleteParallelism(2).forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();

        HoodieJavaWriteClient<HoodieAvroPayload> client = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(Config.hadoopConf), cfg);

        // inserts
        String newCommitTime = client.startCommit();
        LOG.info("Starting commit " + newCommitTime);
        List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
        for(int i =1 ;i<20;i++) {
            final GenericData.Record record = new GenericData.Record(avroSchema);
            record.put("id", Long.valueOf(i));
            record.put("name", "userx x:"+i);
            record.put("age", 10+i);
            record.put("job", "teache x:"+i);
            record.put("salary", 333343.33+i*10);
            HoodieAvroPayload hoodieAvroPayload = new HoodieAvroPayload(Option.of(record));
            HoodieKey hoodieKey = new HoodieKey(String.valueOf(i), "");
            HoodieRecord<HoodieAvroPayload> hoodieRecord = new HoodieAvroRecord<>(hoodieKey, hoodieAvroPayload);
            records.add(hoodieRecord);
            LOG.info(record);
        }

        List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);

        client.upsert(recordsSoFar, newCommitTime);



        //// updates
        //newCommitTime = client.startCommit();
        //LOG.info("Starting commit " + newCommitTime);
        //List<HoodieRecord<HoodieAvroPayload>> toBeUpdated = dataGen.generateUpdates(newCommitTime, 2);
        //records.addAll(toBeUpdated);
        //recordsSoFar.addAll(toBeUpdated);
        //writeRecords =
        //        recordsSoFar.stream().map(r -> new HoodieRecord<HoodieAvroPayload>(r)).collect(Collectors.toList());
        //client.upsert(writeRecords, newCommitTime);
        //
        //// Delete
        //newCommitTime = client.startCommit();
        //LOG.info("Starting commit " + newCommitTime);
        //// just delete half of the records
        //int numToDelete = recordsSoFar.size() / 2;
        //List<HoodieKey> toBeDeleted =
        //        recordsSoFar.stream().map(HoodieRecord::getKey).limit(numToDelete).collect(Collectors.toList());
        //client.delete(toBeDeleted, newCommitTime);

        client.close();
    }
}
