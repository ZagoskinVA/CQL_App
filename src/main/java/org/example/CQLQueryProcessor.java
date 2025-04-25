package org.example;

import com.example.CmdLineWrapper;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.stream.Stream;


public class CQLQueryProcessor extends AbstractProcessor {

    public static final Validator Test_validator = new Validator() {
        public ValidationResult validate(String subject, String value, ValidationContext context) {
            int count = Integer.parseInt(context.getProperty(DataSourceCount).getValue());
            return (new ValidationResult.Builder()).subject(subject).input(value).valid(value != null && !value.isEmpty() && Arrays.stream(value.trim().split(",")).count() == count).explanation(subject + " should be equal DataSourceCount prop").build();
        }
    };

    public static final PropertyDescriptor DataSourceCount = new PropertyDescriptor
            .Builder().name("DataSourceCount")
            .displayName("DataSourceCount")
            .description("Number of input flow files")
            .required(true)
            .addValidator(org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor DataSourceNames = new PropertyDescriptor
            .Builder().name("DataSourceNames")
            .displayName("DataSourceNames")
            .description("The name of each input flow file; if there are several files, the names must be listed separated by commas")
            .required(true)
            .addValidator(Test_validator)
            .build();


    public static final PropertyDescriptor CQLScript = new PropertyDescriptor
            .Builder().name("CQLScript")
            .displayName("CQLScript")
            .description("The CQL query to execute. It will be use content from incoming flow files and write result in output flow files")
            .required(true)
            .addValidator(org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OutputPath = new PropertyDescriptor
            .Builder().name("OutputPath")
            .displayName("Export Path")
            .description("Export path for result after executing CQL query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor InputPath = new PropertyDescriptor
            .Builder().name("InputPath")
            .displayName("Import Path")
            .description("Path for importing file before executing CQL query")
            .required(true)
            .addValidator(org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Successful processing")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;



    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DataSourceNames);
        descriptors.add(DataSourceCount);
        descriptors.add(CQLScript);
        //descriptors.add(InputPath);
        //descriptors.add(OutputPath);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        int dataSourceCount = Integer.parseInt(context.getProperty(DataSourceCount).evaluateAttributeExpressions().getValue());

        var flowFiles = new LinkedList<FlowFile>();

        boolean isWaitFiles = false;
        for(int i = 0; i < dataSourceCount; i++)
        {
            FlowFile flowFile = session.get();

            if(flowFile == null) {
                flowFiles.forEach(file -> {
                    session.transfer(flowFile, Relationship.SELF);
                });

                isWaitFiles = true;
                break;
            } else {
                flowFiles.add(flowFile);
            }
        }

        if(isWaitFiles) {
            return;
        }

        String inputPath = context.getProperty(InputPath).evaluateAttributeExpressions().getValue();
        String outPutPath = context.getProperty(OutputPath).evaluateAttributeExpressions().getValue();
        List<String> fileNames = Arrays.stream(context.getProperty(DataSourceNames).evaluateAttributeExpressions().getValue().split(",")).toList();
        CreateDirs(inputPath);
        CreateDirs(outPutPath);

        var contents = new LinkedList<String>();

        flowFiles.forEach(flowFile -> {
            session.read(flowFile, in -> {
                var stringBuilder = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                    reader.lines().forEach(line -> stringBuilder.append(line).append("\n"));
                }

                contents.add(stringBuilder.toString());
            });
        });

        for(int i = 0; i < contents.stream().count(); i++)
        {
            var content = contents.get(i).toString();
            var path = Paths.get(inputPath);
            var file = new File(path.toFile(), fileNames.get(i) + ".csv");

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                writer.write(content);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        var cqlScript = context.getProperty(CQLScript).evaluateAttributeExpressions().getValue();
        String[] args = new String[1];
        args[0] = cqlScript;
        CmdLineWrapper.main(args);

        var outputDir = Paths.get(outPutPath);

        if(Files.exists(outputDir) && Files.isDirectory(outputDir)) {
            try (Stream<Path> paths = Files.list(outputDir)) {
                paths.filter(Files::isRegularFile)  // только файлы (не директории)
                        .forEach(file -> {
                            FlowFile resultFlowFile = session.create();
                            resultFlowFile = session.write(resultFlowFile, out -> {
                                try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                                    var stringBuilder = new StringBuilder();
                                    reader.lines().forEach(line -> stringBuilder.append(line).append("\n"));
                                    out.write(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
                                }
                            });
                            session.transfer(resultFlowFile, SUCCESS);
                        });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Remove originals
        flowFiles.forEach(flowFile -> {
            session.remove(flowFile);
        });
    }

    private void CreateDirs(String strPath) {
        var path = Paths.get(strPath);

        if(Files.exists(path) && Files.isDirectory(path))
        {
            try {
                Files.walk(path)
                        .filter(Files::isRegularFile)
                        .forEach(file -> {
                            try {
                                Files.delete(file);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
