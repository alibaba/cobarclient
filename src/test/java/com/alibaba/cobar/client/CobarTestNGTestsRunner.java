package com.alibaba.cobar.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.TestNG;

public class CobarTestNGTestsRunner {

    /**
     * @param args
     */
    public static void main(String[] args) {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        TestNG testng = new TestNG();

        List<String> suites = new ArrayList<String>();
        suites.add("src/test/resources/testng.xml");
        testng.setTestSuites(suites);
        testng.setOutputDirectory("target/test-output");
        testng.run();
    }

}
