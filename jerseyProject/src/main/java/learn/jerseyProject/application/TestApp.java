package learn.jerseyProject.application;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.server.ResourceConfig;

@ApplicationPath("/")
public class TestApp extends ResourceConfig {
    public TestApp() {
        packages("learn.jerseyProject.jersey");
        register(LoggingFilter.class);
    }
}
