package tech.tablesaw.pandas.dataframe;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;

public class DataFrameTest {

    @Test
    public void DataFrame() {
        DataFrame dataFrame = new DataFrame();
        System.out.println(dataFrame);

        dataFrame = new DataFrame(Arrays.asList(new Entity1(),
                new Entity1(),
                new Entity1()));
        System.out.println(dataFrame);
    }

    public class Entity1 {
        private int a = 1;
        private boolean b = false;
        private Date c = new Date();
        private float e = 1.0f;
        private double f = 2.0;
        private String g = "abc";
    }
}
