package nndmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class NNDMRTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("dimVector",50);
        conf.setInt("numVectors", 10000);

        conf.set("input", "data/test_dataset");
        conf.set("output", "data/nndmr");

        conf.setInt("k", 20);
        conf.setFloat("sample", 0.5f);
        conf.setFloat("et", 0.001f);

        ToolRunner.run(conf, new NNDMR(), new String[]{});
    }
}