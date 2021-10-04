package com.akgarg.apachecassandraloader;

import com.akgarg.apachecassandraloader.author.Author;
import com.akgarg.apachecassandraloader.author.AuthorRepository;
import com.akgarg.apachecassandraloader.connection.DataStaxAstraProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.nio.file.Path;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class ApacheCassandraLoaderApplication {

    @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
    @Autowired
    private AuthorRepository authorRepository;


    public static void main(String[] args) {
        SpringApplication.run(ApacheCassandraLoaderApplication.class, args);
    }


    /*
     * This method is called when the application boot is complete
     * @PostConstruct annotation is used to execute method after initialization of class object &
     * its lifecycle is maintained by the Spring IoC container
     * */
    @PostConstruct
    public void uploadDataToCassandra() {
        Author author = new Author();
        author.setId("1234");
        author.setName("Author Name");
        author.setPersonalName("Author Personal Name");
        this.authorRepository.save(author);
    }


    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties dataStaxAstraProperties) {
        Path bundle = dataStaxAstraProperties.getSecureConnectBundle().toPath();
        return cqlSessionBuilder -> cqlSessionBuilder.withCloudSecureConnectBundle(bundle);
    }
}
