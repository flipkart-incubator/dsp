package com.flipkart.dsp.config;

import com.google.inject.Binder;

import com.hubspot.dropwizard.guicier.DropwizardAwareModule;

import org.reflections.Reflections;

import java.util.Set;

import javax.ws.rs.Path;

import io.dropwizard.Configuration;

import static java.util.Arrays.asList;

public class JerseyResourceModule<T extends Configuration> extends DropwizardAwareModule<T> {

  private String[] packages;

  public JerseyResourceModule(String... packages) {

    this.packages = packages;
  }

  @Override
  public void configure(Binder binder) {
    bindResources(binder);
  }

  private void bindResources(Binder binder) {

    asList(packages).stream().forEach(pkg -> {
      Reflections reflections = new Reflections(pkg);
      Set<Class<?>> resourceClasses = reflections.getTypesAnnotatedWith(Path.class);
      resourceClasses.stream().forEach(c -> binder.bind(c));
    });
  }
}
