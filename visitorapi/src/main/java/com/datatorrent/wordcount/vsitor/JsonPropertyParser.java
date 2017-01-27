package com.datatorrent.wordcount.vsitor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;

public class JsonPropertyParser
{
  private String path;
  private Map<String, Map<String, String>> properties = new HashMap<>();

  public JsonPropertyParser(String path) throws FileNotFoundException
  {
    this(new FileInputStream(path));
  }

  public JsonPropertyParser(InputStream in)
  {
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readTree(in);
      Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.fields();
      while (fieldsIterator.hasNext()) {
        Map.Entry<String, JsonNode> field = fieldsIterator.next();
        if (field.getKey().equals("operators")) {
          getOperatorProperties(field.getValue());
        }
      }
    } catch (JsonProcessingException e1) {
      e1.printStackTrace();
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
    } catch (IOException e1) {
      e1.printStackTrace();
    }
  }

  private Map<String, Map<String, String>> getOperatorProperties(JsonNode value)
  {
    if (value.getNodeType() == JsonNodeType.OBJECT) {
      for (Iterator<Map.Entry<String, JsonNode>> it = value.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> field = it.next();
        properties.put(field.getKey(), extractProperties(field.getValue()));
        System.out.println("operator name " + field.getKey());
      }
      //System.out.println(child);
    }
    return null;
  }

  private Map<String, String> extractProperties(JsonNode value)
  {
    Map<String, String> keyVal = new HashMap<>();
    if (value.getNodeType() == JsonNodeType.OBJECT) {
      for (Iterator<Map.Entry<String, JsonNode>> it = value.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> field = it.next();
        keyVal.put(field.getKey(), field.getValue().asText());
      }
    }
    return keyVal;
  }

  Map<String, Map<String, String>> getProperties()
  {
    return properties;
  }

  public static void main(String[] args) throws FileNotFoundException
  {
    JsonPropertyParser parser = new JsonPropertyParser("/tmp/test.json");
    System.out.println(parser.getProperties());
  }
}
