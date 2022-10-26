import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class BeamInnerJoinExample01 {

    public static void main(String[] args) {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline        p = Pipeline.create(options);

        Schema schema = new BeamShemaUtil("data/ship_data_schema.avsc").convertAvroBeamSchema();
        ValueProvider<Schema> schema2 = ValueProvider.StaticValueProvider.of(schema);

        PCollection<String> pc1 = p.apply(TextIO.read().from("data/ship_data.csv"));
        PCollection<Row>    pc2 = pc1.apply(ParDo.of(new ReadCsvExample01.CSVRowConverter(schema2))).setRowSchema(schema);

        PCollection<Row>    pc_left  = pc2.apply(SqlTransform.query("select Ship_name, Age from PCOLLECTION"));
        PCollection<Row>    pc_right = pc2.apply(SqlTransform.query("select Ship_name, Tonnage, cabins, crew from PCOLLECTION"));

        System.out.println(pc_right.getSchema().toString());
        System.out.println(pc_left.getSchema().toString());

        PCollection<Row> joined = pc_left.apply(Join.<Row,Row>innerJoin(pc_right).using("Ship_name"));

        joined.apply(ParDo.of(new ReadCsvExample01.RowPrinter()));


        PipelineResult result = p.run();
        result.waitUntilFinish();


    }
}
