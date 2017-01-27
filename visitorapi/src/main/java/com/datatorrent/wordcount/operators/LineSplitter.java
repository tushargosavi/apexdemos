package com.datatorrent.wordcount.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class LineSplitter extends BaseOperator
{
  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<>();

  private String regex = " ";

  public transient final DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      for (String part : s.split(regex)) {
        output.emit(part);
      }
    }
  };

  public String getRegex()
  {
    return regex;
  }

  public void setRegex(String regex)
  {
    this.regex = regex;
  }
}
