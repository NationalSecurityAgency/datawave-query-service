package datawave.microservice.query.mapreduce.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;

@Configuration
@ConditionalOnProperty(name = MapReduceQueryProperties.PREFIX + ".enabled", havingValue = "true", matchIfMissing = true)
public class MapReduceQueryControllerConfig {
    @Bean
    public WebSecurityCustomizer ignoreMapReduceUpdateState() {
        return (web) -> web.ignoring().antMatchers("/v1/mapreduce/updateState");
    }
}
