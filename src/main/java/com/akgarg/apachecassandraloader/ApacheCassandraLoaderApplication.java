package com.akgarg.apachecassandraloader;

import com.akgarg.apachecassandraloader.author.Author;
import com.akgarg.apachecassandraloader.author.AuthorRepository;
import com.akgarg.apachecassandraloader.connection.DataStaxAstraProperties;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class ApacheCassandraLoaderApplication {

    /*
     * This method is called when the application boot is complete
     * @PostConstruct annotation is used to execute method after initialization of class object &
     * its lifecycle is maintained by the Spring IoC container
     * */
    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.work}")
    private String workDumpLocation;

    @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
    @Autowired
    private AuthorRepository authorRepository;

    public static void main(String[] args) {
        SpringApplication.run(ApacheCassandraLoaderApplication.class, args);
    }

    @SuppressWarnings({"GrazieInspection", "CommentedOutCode"})
    @PostConstruct
    public void uploadDataToCassandra() {
        // Author author = new Author();
        // author.setId("1234");
        // author.setName("Author Name");
        // author.setPersonalName("Author Personal Name");
        // this.authorRepository.save(author);
        initAuthors();
        initWorks();
    }

    private void initWorks() {

    }

    private void initAuthors() {
        Path path = Paths.get(authorDumpLocation);
        try {
            Stream<String> lines = Files.lines(path);
            System.out.println("Upload starts");

            lines.forEach(line -> {
                String jsonString = line.substring(line.indexOf('{'));

                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));
                    this.authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });

            System.out.println("Upload completes");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties dataStaxAstraProperties) {
        Path bundle = dataStaxAstraProperties.getSecureConnectBundle().toPath();
        return cqlSessionBuilder -> cqlSessionBuilder.withCloudSecureConnectBundle(bundle);
    }
}
