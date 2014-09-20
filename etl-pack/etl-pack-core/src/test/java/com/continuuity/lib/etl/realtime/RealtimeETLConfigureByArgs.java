package com.continuuity.lib.etl.realtime;

// Everything configured by args
public class RealtimeETLConfigureByArgs extends RealtimeETL {
  @Override
  protected void configure(Configurer configurer) {
    // there's no way to set it with args
    configurer.setInputStream("userDetailsStream");
  }
}
