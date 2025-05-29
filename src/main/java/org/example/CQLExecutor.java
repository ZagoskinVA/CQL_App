package org.example;
import catdata.Program;
import catdata.Util;
import catdata.cql.AqlOptions.AqlOption;
import catdata.cql.exp.AqlEnv;
import catdata.cql.exp.AqlMultiDriver;
import catdata.cql.exp.AqlParserFactory;
import catdata.cql.exp.AqlTyping;
import catdata.cql.exp.Exp;

import java.nio.file.Files;
import java.nio.file.Path;

public class CQLExecutor {
    public static void Execute(String cqlQuery)
    {
        try {
        Program<Exp<?>> program = AqlParserFactory.getParser().parseProgram(cqlQuery);
        AqlEnv env = new AqlEnv(program);
        env.typing = new AqlTyping(program, false);
        AqlMultiDriver d = new AqlMultiDriver(program, env);
        d.start();

        String html = "";
        for (String n : program.order) {
            Exp<?> exp = program.exps.get(n);
            Object val = env.get(exp.kind(), n);
            if (val == null) {
                html += exp.kind() + " " + n + " = no result for " + n;
            } else {
                html += exp.kind() + " " + n + " = " + val + "\n\n";
            }
        }
        System.out.println(html.trim());
        } catch (Throwable ex) {
            ex.printStackTrace();
            System.out.println("ERROR " + ex.getMessage());
        }
    }
}
