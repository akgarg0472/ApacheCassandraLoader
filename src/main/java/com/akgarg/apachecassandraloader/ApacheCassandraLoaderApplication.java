package com.akgarg.apachecassandraloader;

import com.akgarg.apachecassandraloader.author.Author;
import com.akgarg.apachecassandraloader.author.AuthorRepository;
import com.akgarg.apachecassandraloader.book.Book;
import com.akgarg.apachecassandraloader.book.BookRepository;
import com.akgarg.apachecassandraloader.connection.DataStaxAstraProperties;
import org.json.JSONArray;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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

    @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
    @Autowired
    private BookRepository bookRepository;

    public static void main(String[] args) {
        SpringApplication.run(ApacheCassandraLoaderApplication.class, args);
    }

    @PostConstruct
    public void uploadDataToCassandra() {
        // initAuthors();
        initWorks();
    }

    private void initWorks() {
        Path path = Paths.get(workDumpLocation);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

        try {
            Stream<String> lines = Files.lines(path);
            lines.forEach(line -> {
                String jsonString = line.substring(line.indexOf('{'));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    Book book = new Book();

                    book.setId(jsonObject.getString("key").replace("/works/", ""));
                    book.setName(jsonObject.optString("title"));

                    JSONObject descriptionObject = jsonObject.optJSONObject("description");
                    if (descriptionObject != null) {
                        book.setDescription(descriptionObject.optString("value"));
                    }

                    JSONObject publishedObject = jsonObject.getJSONObject("created");
                    if (publishedObject != null) {
                        String dateCreated = publishedObject.getString("value");
                        book.setPublishDate(LocalDate.parse(dateCreated, dateTimeFormatter));
                    }

                    JSONArray coversArray = jsonObject.optJSONArray("covers");
                    if (coversArray != null) {
                        List<String> coverIds = new ArrayList<>();
                        for (int i = 0; i < coversArray.length(); i++) {
                            coverIds.add(coversArray.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }

                    JSONArray authorsArray = jsonObject.optJSONArray("authors");
                    if (authorsArray != null) {
                        List<String> authorIds = new ArrayList<>();
                        for (int i = 0; i < authorsArray.length(); i++) {
                            authorIds.add(authorsArray.getJSONObject(i).getJSONObject("author")
                                    .getString("key").replace("/authors/", ""));
                        }
                        book.setAuthorIds(authorIds);

                        List<String> authorNames = authorIds.stream().map(id -> this.authorRepository.findById(id))
                                .map(optionalAuthor -> optionalAuthor.isPresent() ?
                                        optionalAuthor.get().getName() :
                                        "Unknown Author"
                                ).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }
                    this.bookRepository.save(book);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
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
