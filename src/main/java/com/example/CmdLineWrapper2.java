package com.example;

import catdata.Program;
import catdata.Util;
import catdata.cql.exp.AqlEnv;
import catdata.cql.exp.AqlMultiDriver;
import catdata.cql.exp.AqlParserFactory;
import catdata.cql.exp.AqlTyping;
import catdata.cql.exp.Exp;

public class CmdLineWrapper2 {

	public static void main(String[] args) {
		 System.out.println("-------------------------------------------------------------");
         
		 try {
		      String s = "options\n"
		      		+ "	always_reload = true\n"
		      		+ "timeout=300000\n"
		      		+ "schema S = literal : sql {\n"
		      		+ "	entities\n"
		      		+ "		Employee\n"
		      		+ "		Department\n"
		      		+ "	foreign_keys\n"
		      		+ "		manager   : Employee -> Employee\n"
		      		+ "		worksIn   : Employee -> Department\n"
		      		+ "		secretary : Department -> Employee\n"
		      		+ "	path_equations\n"
		      		+ "		Employee.manager.worksIn = Employee.worksIn\n"
		      		+ "  		Department.secretary.worksIn = Department\n"
		      		+ "  	attributes\n"
		      		+ "  		first last	: Employee -> String\n"
		      		+ "     	age			: Employee -> Integer\n"
		      		+ "     	name 		: Department -> String\n"
		      		+ "}\n"
		      		+ "\n"
		      		+ "instance I = literal : S {\n"
		      		+ "	generators\n"
		      		+ "		a b c : Employee\n"
		      		+ "		m s : Department\n"
		      		+ "	equations\n"
		      		+ "		first(a) = Al\n"
		      		+ "		first(b) = Bob  last(b) = Bo\n"
		      		+ "		first(c) = Carl\n"
		      		+ "		name(m)  = Math name(s) = CS\n"
		      		+ "		age(a) = age(c)\n"
		      		+ "		manager(a) = b manager(b) = b manager(c) = c\n"
		      		+ "		worksIn(a) = m worksIn(b) = m worksIn(c) = s\n"
		      		+ "		secretary(s) = c secretary(m) = b\n"
		      		+ "		secretary(worksIn(a)) = manager(a)\n"
		      		+ "		worksIn(a) = worksIn(manager(a))\n"
		      		+ "		age(a) = \"2\"\n"
		      		+ "		age(manager(a)) = \"1\"\n"
		      		+ "}\n"
		      		+ "\n"
		      		+ "command exportRdfData = export_rdf_instance_xml I \"file.xml\" {\n"
		      		+ "	external_types\n"
		      		+ "		Integer -> \"http://www.w3.org/2001/XMLSchema#Integer\" \"x => x.toString()\"\n"
		      		+ "}\n"
		      		+ "\n"
		      		+ "instance J = import_rdf_all \"file.xml\" #can import from .ttl too\n"
		      		+ "\n"
		      		+ "instance K = spanify J"
		      		+ "\n"
		      		+ "command exportCsvData = export_csv_instance I \"LIT_exported\"\n"
		      		+ "\n"
		      		+ "command exportCsvData2 = export_csv_instance K \"RDF_exported\"\n";
		      
		      Program<Exp<?>> program = AqlParserFactory.getParser().parseProgram(s);
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
