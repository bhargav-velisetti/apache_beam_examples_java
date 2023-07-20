import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.joda.time.Duration;

import javax.annotation.concurrent.Immutable;
import java.util.HashMap;
import java.util.Map;

public class ReadKafkaExample01 {


    public static void main(String[] args) {

        Pipeline p = Pipeline.create();

        Map<String, Object> a = new HashMap();
        a.put("group.id","some-group-id");
        a.put("enable.auto.commit", "false");
        a.put("auto.offset.reset", "earliest");
        PCollection<KafkaRecord<Integer, String>> pckafka = p.apply(KafkaIO.<Integer, String>read()
                                                        .commitOffsetsInFinalize()
                                                        .withBootstrapServers("localhost:9092")
                                                        .withConsumerConfigUpdates(a)
                                                        .withTopic("test_producer01")
                                                        .withKeyDeserializer(IntegerDeserializer.class)
                                                        .withValueDeserializer(StringDeserializer.class));

        pckafka.apply(ParDo.of(new PrintKafkaMessages()));

        PipelineResult result = p.run();
        result.waitUntilFinish(Duration.standardSeconds(120)); // Pipeline will run 2 mins only
    }
    public static class PrintKafkaMessages extends DoFn<KafkaRecord<Integer, String>,Void>{

        @ProcessElement
        public void processElement(ProcessContext  c) {

            KafkaRecord<Integer, String> record = c.element();
            System.out.println("offset = " + record.getOffset() + " data = " + record.getKV().getValue() + " TS= " + record.getTimestamp());
           // record.getOffset();
           // record.getKV().getValue();
            /*
            KV<Integer,String> record2 =  record.getKV();

            Long processed_tmp  = record.getTimestamp();
            String topic        = record.getTopic();
            Integer key         = record2.getKey();
            String value        = record2.getValue();

            System.out.println("Topic = " + topic + " Fed to beam at unix epoch = " + processed_tmp + " message key " + key + " message content " + value );

             */
        }
    }
}