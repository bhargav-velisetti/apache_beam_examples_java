import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.utils.AvroUtils;


public class ReadAvroExample01 {

    public static void main(String[] args)  {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        options.setRunner(DirectRunner.class);

        String schemaFile = options.getSchemaFile();
        String inputFile  = options.getInputFile();

        Pipeline p = Pipeline.create(options);

        org.apache.avro.Schema schema = new BeamShemaUtil(schemaFile).getAvroSchema();
        org.apache.beam.sdk.schemas.Schema beam_schema = new BeamShemaUtil(schemaFile).convertAvroBeamSchema();

        PCollection<GenericRecord> pc1 = p.apply(AvroIO.readGenericRecords(schema).withBeamSchemas(true).from(inputFile));
        PCollection<String>        pc2 = pc1.apply(ParDo.of(new GenericRecordPrinter()));
        PCollection<Row>           pc3 = pc1.apply(MapElements.via(new GenericRecordtoRowConverter(beam_schema))).setRowSchema(beam_schema) ;

        PipelineResult result = p.run();
        result.waitUntilFinish();

    }

    public static class GenericRecordPrinter extends DoFn<GenericRecord, String> {
        @ProcessElement
        public void  processElement(ProcessContext c) {
            System.out.println(c.element().toString());
            c.output(c.element().toString());

        }
    }

    public static class GenericRecordtoRowConverter extends SimpleFunction<GenericRecord, Row> {
        Schema InputBeamSchema;
        public GenericRecordtoRowConverter(Schema InputBeamSchema) {
            this.InputBeamSchema = InputBeamSchema;
        }

        @Override
        public Row apply(GenericRecord record) {
           return AvroUtils.getGenericRecordToRowFunction(InputBeamSchema).apply(record);
        }

    }

}
