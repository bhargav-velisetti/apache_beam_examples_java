import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
//import java.util.Map;


public class ReadParuetExample01 {

    public static void main(String[] args) {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline p = Pipeline.create(options);

        org.apache.avro.Schema schema = new BeamShemaUtil(options.getSchemaFile()).getAvroSchema();

        PCollection<GenericRecord> pc1 =   p.apply(ParquetIO.read(schema)
                                            .from(options.getInputFile())
                                           // .withConfiguration(Map.of("parquet.avro.readInt96AsFixed", "true"))
                                            .withBeamSchemas(true)
                                            );
        PCollection<String>        pc2 = pc1.apply(ParDo.of(new ReadAvroExample01.GenericRecordPrinter()));

        PipelineResult result = p.run();
        result.waitUntilFinish();


    }
}
