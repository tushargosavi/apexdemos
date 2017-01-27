package com.datatorrent.wordcount.vsitor;

import java.io.FileNotFoundException;
import java.util.Map;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

public class JsonPropertyInjector implements DAG.Visitor
{
  String path;
  private Map<String, Map<String, String>> properties;

  public JsonPropertyInjector(String path)
  {
    this.path = path;
  }

  @Override
  public void preVisitDAG(DAG dag)
  {
    JsonPropertyParser parser = null;
    parser = new JsonPropertyParser(getClass().getResourceAsStream(path));
    properties = parser.getProperties();
  }

  @Override
  public void visitOperator(DAG.OperatorMeta operatorMeta)
  {
    if (properties.containsKey(operatorMeta.getName())) {
      Operator o = operatorMeta.getOperator();
      LogicalPlanConfiguration.setOperatorProperties(o, properties.get(operatorMeta.getName()));
    }
  }

  @Override
  public void visitStream(DAG.StreamMeta streamMeta)
  {

  }

  @Override
  public void postVisitDAG()
  {

  }
}
