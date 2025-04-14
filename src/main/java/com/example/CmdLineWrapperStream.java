package com.example;

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

public class CmdLineWrapperStream {

	public static void main(String[] args) {
		 System.out.println("-------------------------------------------------------------");
         
		 try {
		      String s = "options\n"
		      		+ "	always_reload = true\n"
		      		+ "timeout=300000\n"
		      		+ "\n"
		      		+ "schema S0 = literal : sql {\n"
		      		+ "	entities\n"
		      		+ "		Employee\n"
		      		+ "		Person\n"
		      		+ "	foreign_keys\n"
		      		+ "		eAsP : Employee -> Person\n"
		      		+ "	attributes\n"
		      		+ "		ssn : Person -> Integer\n"
		      		+ "		eId : Employee -> Integer\n"
		      		+ "		eSsn  : Employee -> Integer\n"
		      		+ "	observation_equations\n"
		      		+ "		eSsn = eAsP . ssn\n"
		      		+ "}\n"
		      		+ "\n"
		      		+ "#  Person.csv is the file\n"
		      		+ "#   pId\n"
		      		+ "#   0\n"
		      		+ "#   1\n"
		      		+ "#   2\n"
		      		+ "#\n"
		      		+ "#   Employee.csv is the file\n"
		      		+ "#    eId,is\n"
		      		+ "#    10,0\n"
		      		+ "#    11,1\n"
		      		+ "#    12,2\n"
		      		+ "\n"
		      		+ "command createCsvData = exec_js {\n"
		      		+ "	\"Java.type(\\\"catdata.Util\\\").writeFile(\\\"pId\\\\n0\\\\n1\\\\n2\\\", \\\"Person.csv\\\")\"\n"
		      		+ "	\"Java.type(\\\"catdata.Util\\\").writeFile(\\\"eId,is\\\\n10,0\\\\n11,1\\\\n12,2\\\", \\\"Employee.csv\\\")\"\n"
		      		+ "}\n"
		      		+ "\n"
		      		+ "instance I0 = import_stream \".\" : S0 {\n"
		      		+ "	Employee -> {Employee -> eId   eAsP -> is    eSsn -> is}\n"
		      		+ "	#eId -> eId can be ommitted\n"
		      		+ "\n"
		      		+ "	Person -> {Person -> pId  ssn -> pId}\n"
		      		+ "}\n"
		      		+ "\n"
		      		+ "command exportCsvData = export_csv_instance I0 \"exported\"\n"
		      		+ "command exportCsvData2 = export_csv_transform (identity I0) \"exported_trans.csv\"";


			  var str = Files.readString(Path.of("D:\\cql\\program.txt"));
		      Program<Exp<?>> program = AqlParserFactory.getParser().parseProgram(str);
		      AqlEnv env = new AqlEnv(program);
		      env.typing = new AqlTyping(program, false);
		      AqlMultiDriver d = new AqlMultiDriver(program, env);
		      //env.defaults.options.put(AqlOption.jdbc_default_class, "IO STREAM SHOUD BE PLACED HERE");
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
		        html+="\n";
		      }
		      System.out.println(html.trim());
		    } catch (Throwable ex) {
		      ex.printStackTrace();
		      System.out.println("ERROR " + ex.getMessage());
		    }     
         
         
	}
}
