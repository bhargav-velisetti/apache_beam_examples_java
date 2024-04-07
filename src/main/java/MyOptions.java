import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {

    // --schemaFile=C:\Users\bharg\Documents\dataeng\data\data.csv --inputFile=C:\Users\bharg\Documents\dataeng\output\test     CLI args passed like this

    public void setSchemaFile(String schemaFile);
    public String getSchemaFile();

    public void setInputFile(String inputFile);
    public String getInputFile();

    public void setOutputFile(String outputFile);
    public String getOutputFile();
    public  void setPubSubTopic(String pubSubTopic);
    public String getPubSubTopic();

    public  void  setBQProject(String bQProject);
    public String getBQProject();

    public  void  setBQDataset(String bQDataset);
    public String getBQDataset();

}
