import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;

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

        List<Schema.Field> schema_left = pc_left.getSchema().getFields();
        List<Schema.Field> schema_right = pc_right.getSchema().getFields();


        List<Schema.Field> schema_joined_list = new ArrayList<>();
        for(int i=0;i<schema_left.size();i++){
            schema_joined_list.add(schema_left.get(i));
        }
        for(int i=1;i<schema_right.size();i++){
            schema_joined_list.add(schema_right.get(i));
        }

        Schema schemaJoined = Schema.builder().addFields(schema_joined_list).build();
        System.out.println(schemaJoined.toString());
        PCollection<Row> joined = pc_left.apply(Join.<Row,Row>innerJoin(pc_right).using("Ship_name"));

        PCollection<Row> flatndJoin = joined.apply(ParDo.of(new FlattenJoinedPcol(schemaJoined)))
                                            .setRowSchema(schemaJoined);

        flatndJoin.apply(ParDo.of(new ReadCsvExample01.RowPrinter()));
        System.out.println(flatndJoin.getSchema().toString());


        PipelineResult result = p.run();
        result.waitUntilFinish();
    }

    public static class FlattenJoinedPcol extends DoFn<Row,Row> {

        static Schema schema;
        public FlattenJoinedPcol(Schema schema) {
            this.schema = schema;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {

            Row outer_row = c.element();
            Row lhs_row = outer_row.getRow("lhs");
            Row rhs_row = outer_row.getRow("rhs");

            Row.Builder joinedRow = Row.withSchema(schema);
            joinedRow.addValues(lhs_row.getValues());

            for (int i=1;i<rhs_row.getFieldCount();i++) {
                joinedRow.addValue(rhs_row.getValue(i));
            }
            c.output(joinedRow.build()); ;

        }
    }

}
