package com.example;

import catdata.Program;
import catdata.Util;
import catdata.cql.exp.AqlEnv;
import catdata.cql.exp.AqlMultiDriver;
import catdata.cql.exp.AqlParserFactory;
import catdata.cql.exp.AqlTyping;
import catdata.cql.exp.Exp;
import org.apache.nifi.components.PropertyDescriptor;

import java.util.Arrays;

public class CmdLineWrapper {

	public static void main(String[] args) {
		 System.out.println("-------------------------------------------------------------");
         
		 try {
			 var script = Arrays.stream(args).count() > 0 ? args[0] : null;

			 System.out.println(String.format("Args count: %d\n", args.length));
			 System.out.println(String.format("Script: %s\n", script));

			  String code = script != null ? script : """
					  options
					  	always_reload = true
					  timeout=300000
					  
					  
					  schema Customer = literal: sql {
					  	entities\s
					  		PurchaseCustomer
					  		WebCustomer
					  		DemCustomer
					  	foreign_keys
					  		customer_id: WebCustomer -> PurchaseCustomer
					  		customer_id: PurchaseCustomer -> WebCustomer
					  		customer_id: DemCustomer -> PurchaseCustomer
					  	attributes
					  		total_spent : PurchaseCustomer -> Integer
					  		pages_visited : WebCustomer -> Integer
					  		cart_abandon_rate : WebCustomer -> Integer
					      		age : DemCustomer -> Integer
					      		gender : DemCustomer -> String
					  		customer_id : PurchaseCustomer -> Integer
					  }
					  
					  
					  
					  instance CustomerInstance = import_csv "/tmp/cql/import" : Customer {
					  	PurchaseCustomer -> { PurchaseCustomer -> customer_id  customer_id -> customer_id total_spent -> total_spent}\s
					  	WebCustomer -> { WebCustomer -> customer_id customer_id -> customer_id pages_visited -> pages_visited cart_abandon_rate -> cart_abandon_rate }
					  	DemCustomer -> { DemCustomer ->  customer_id  customer_id -> customer_id age -> age gender -> gender}
					  }
					  
					  schema FullCustomerProfile = literal : sql {
					    entities Customer
					    attributes
					      customer_id : Customer -> Integer
					      total_spent : Customer -> Integer
					      pages_visited : Customer -> Integer
					      cart_abandon_rate : Customer -> Integer
					      age : Customer -> Integer
					      gender : Customer -> String
					  }
					  
					  
					  mapping CustomerToFullProfile = literal : FullCustomerProfile -> Customer {
					  entity
					  	Customer -> DemCustomer
					  	attributes
					  		customer_id -> customer_id.customer_id
					  		total_spent -> customer_id.total_spent
					  		pages_visited -> customer_id.customer_id.pages_visited
					  		cart_abandon_rate -> customer_id.customer_id.cart_abandon_rate
					  		age -> age
					  		gender -> gender
					  
					  }
					  
					  
					  instance FullProfile = delta CustomerToFullProfile CustomerInstance
					  command exportCsvData3 = export_csv_instance FullProfile "/tmp/cql/result/out"
					  """;
		      Program<Exp<?>> program = AqlParserFactory.getParser().parseProgram(code);
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
