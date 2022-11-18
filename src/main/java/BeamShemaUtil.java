import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

public class BeamShemaUtil  {
    String path;
   public BeamShemaUtil(String path) {
       this.path = path ;
   }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }


    public org.apache.avro.Schema getAvroSchema()  {

       try {
           org.apache.avro.Schema avro_schema =  new Schema.Parser().parse(new File(this.path));
           return avro_schema;
       } catch (IOException e) {
           throw new RuntimeException(e);
       }
    }

    public org.apache.beam.sdk.schemas.Schema convertAvroBeamSchema() {

        return AvroUtils.toBeamSchema(getAvroSchema());
    }


    public  com.google.api.services.bigquery.model.TableSchema convertBQTableSchema() {
       return BigQueryUtils.toTableSchema(convertAvroBeamSchema());
    }




}
