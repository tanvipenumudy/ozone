package org.apache.hadoop.ozone.om.lock;

import org.apache.hadoop.ozone.om.hashcodegenerator.OMHashCodeGenerator;
import org.apache.hadoop.ozone.om.hashcodegenerator.StringOMHashCodeGeneratorImpl;

public class OMHashCodeGeneratorFactory {

  // take config as a param
  public static OMHashCodeGenerator getOMHashCodeGenerator(Class<?> cl) {
    if (cl == StringOMHashCodeGeneratorImpl.class) {
      return new StringOMHashCodeGeneratorImpl();
    }
    return null;
  }
}
