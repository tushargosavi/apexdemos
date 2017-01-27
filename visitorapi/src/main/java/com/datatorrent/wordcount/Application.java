package com.datatorrent.wordcount;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.wordcount.operators.LineSplitter;

@ApplicationAnnotation(name = "VisitorTestApp")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    LineByLineFileInputOperator input = dag.addOperator("Input", new LineByLineFileInputOperator());
    LineSplitter splitter = dag.addOperator("Splitter", new LineSplitter());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("s1", input.output, splitter.input);
    dag.addStream("s2", splitter.output, console.input);
  }
}
