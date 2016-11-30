package com.acme.luxae;

import java.util.List;
import junit.framework.Assert;
import java.util.Map;
import org.junit.Test;

/**
 * @author Chernyshev
 */
public class SparkAppTest {
    
    static final String input = "./data/example_input.txt";
    static final String modifiers = "./data/INSTRUMENT_PRICE_MODIFIER.csv";
    
    @Test
    public void testApp() {
        Map<String, List> result = new SparkApp("mean_all").runWithData(input, modifiers);
        
        // Mean for Instrument1 should be calculated
        Assert.assertTrue( ! result.get("MEAN_INSTRUMENT1").isEmpty());
        // Instrument2 Nov/2014 should be calculated
        Assert.assertTrue( ! result.get("MEAN_INSTRUMENT2_11_2014").isEmpty());
        // All additional metrics should be calculated for all instrument
        Assert.assertTrue( ! result.get("MEAN_ALL").isEmpty() );
        
        System.out.println("Check executed jobs on http://localhost:4040/jobs/");
    }
    
}
