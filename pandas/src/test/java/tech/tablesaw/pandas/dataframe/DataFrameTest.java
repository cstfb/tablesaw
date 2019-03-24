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
        private int aB = 1;
        private boolean b = false;
        private Date c = new Date();
        private float eE = 1.0f;
        private double f = 2.0;
        private String g = "abc";
        private Object eee = null;

        private Integer Ing = null;
        private Boolean b_ = null;
        private Date c_ = null;
        private Float eE_ = null;
        private Double f_ = null;
        private String g_ = null;
    }
}
