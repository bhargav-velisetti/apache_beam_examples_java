import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class ReadCsvExample01 {

    public static void main(String[] args) {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        options.setRunner(DirectRunner.class);
        Pipeline p = Pipeline.create(options) ;

        Schema beam_schema = new BeamShemaUtil(options.getSchemaFile()).convertAvroBeamSchema();
        ValueProvider<Schema> schema = ValueProvider.StaticValueProvider.of(beam_schema);


        System.out.println(beam_schema.toString());


        PCollection<String> pc1 = p.apply(TextIO.read().from(options.getInputFile()));
                            pc1.apply(MapElements.via(new StringPColPrinter() ));
        PCollection<Row>    pc2 = pc1.apply(ParDo.of(new CSVRowConverter(schema))).setRowSchema(beam_schema);
        PCollection<Row>    pc3 = pc2.apply(SqlTransform.query("select name, tx_timestamp from PCOLLECTION"));

        System.out.println(pc3.getSchema().toString());
        pc3.apply(ParDo.of(new RowPrinter()));


        PipelineResult result = p.run();
        result.waitUntilFinish();

    }

    public static class StringPColPrinter extends SimpleFunction<String,String> {
        @Override
        public String apply(String line) {
            System.out.println(line);
            return line;
        }
    }

    public static class CSVRowConverter extends DoFn<String,Row> {
        Schema csv_schema;
        public CSVRowConverter(ValueProvider<Schema> schema) {
            this.csv_schema = schema.get();
        }

        public Row rowBuilder(Schema sc1, String[] arr) {
            Row.Builder row = Row.withSchema(sc1);

            for (int i=0; i< arr.length; i++) {

                String fieldType = sc1.getField(i).getType().toString();

                switch (fieldType) {
                    case "STRING":
                        row.addValue(arr[i]);
                        break;
                    case "BOOLEAN":
                       row.addValue(Boolean.valueOf(arr[i]));
                        break;
                    case "INT32":
                        row.addValue(Integer.valueOf(arr[i]));
                        break;
                    case "INT64":
                        row.addValue(Long.valueOf(arr[i]));
                        break;
                    case "FLOAT":
                        row.addValue(Float.valueOf(arr[i]));
                        break;
                    case "DOUBLE":
                        row.addValue( Double.valueOf(arr[i]));
                    case "DATETIME":
                        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
                        row.addValue(DateTime.parse(arr[i] , formatter) );
                        break;
                    default:

                        throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
                }

            }

            return row.build();

        }

        @ProcessElement
        public void processElement(ProcessContext  c) {
            String arr[] = c.element().split(",");

            Row row = rowBuilder( csv_schema , arr);
            c.output(row);

        }
    }

    public static class RowPrinter extends DoFn<Row,Void> {
        @ProcessElement
        public void processElement(ProcessContext  c) {
          String record =  c.element().toString();
            System.out.println(record);
          //  c.output(null);
        }

    }




 }
