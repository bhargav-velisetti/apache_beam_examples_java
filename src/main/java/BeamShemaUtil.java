import org.apache.avro.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import java.io.File;
import java.io.IOException;

public class BeamShemaUtil {
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




}
