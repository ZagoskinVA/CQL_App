package org.example;

import catdata.Program;
import catdata.cql.exp.AqlParserFactory;
import catdata.cql.exp.CombinatorParser;
import catdata.cql.exp.Exp;
import com.example.CmdLineWrapper;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.StreamCallback;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


public class TestProcessor extends AbstractProcessor {

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("My Property")
            .description("Example Property")
            .required(true)
            .defaultValue("Default Value")
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
        descriptors.add(MY_PROPERTY);
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
        FlowFile flowFile1 = session.get();
        if (flowFile1 == null) return;

        FlowFile flowFile2 = session.get();
        if (flowFile2 == null) {
            session.transfer(flowFile1, Relationship.SELF);
            return;
        }


        FlowFile flowFile3 = session.get();

        if(flowFile3 == null) {
            session.transfer(flowFile1, Relationship.SELF);
            session.transfer(flowFile2, Relationship.SELF);
            return;
        }

        // Read content
        final StringBuilder content1 = new StringBuilder();
        session.read(flowFile1, in -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                reader.lines().forEach(line -> content1.append(line).append("\n"));
            }
        });

        final StringBuilder content2 = new StringBuilder();
        session.read(flowFile2, in -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                reader.lines().forEach(line -> content2.append(line).append("\n"));
            }
        });

        final StringBuilder content3 = new StringBuilder();

        session.read(flowFile3, in -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                reader.lines().forEach(line -> content3.append(line).append("\n"));
            }
        });

        FlowFile resultFlowFile = session.create();
        resultFlowFile = session.write(resultFlowFile, out -> {
            String purchaseDataSource = content1.toString();
            String webDataSource = content2.toString();
            String demDataSource = content3.toString();

            try (BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/cql/import/PurchaseCustomer.csv"))) {
                writer.write(purchaseDataSource);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/cql/import/WebCustomer.csv"))) {
                writer.write(webDataSource);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/cql/import/DemCustomer.csv"))) {
                writer.write(demDataSource);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // RUN CQL example program
            CmdLineWrapper.main(new String[0]);

            try (BufferedReader reader = new BufferedReader(new FileReader("/tmp/cql/result/out/Customer.csv"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    out.write(line.getBytes(StandardCharsets.UTF_8));
                    out.write("\n".getBytes(StandardCharsets.UTF_8));
                }
            }
        });
        session.transfer(resultFlowFile, SUCCESS);

        // Remove originals
        session.remove(flowFile1);
        session.remove(flowFile2);
        session.remove(flowFile3);
    }
}
