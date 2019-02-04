package C3M;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Logger;


public abstract class SparqlQuerier {
	
	private static Logger logger = Logger.getLogger(SparqlQuerier.class);

	protected int limit = 10000;
	protected int offset = 0;
	protected String query;
	protected String triplestore;
	protected String timeout = null;
	protected SparqlStatus status = SparqlStatus.WAIT;

	private int size;
	
	protected static int readTimeout = 8 * 60 * 1000;
	protected static int connectTimeout = 1 * 60 * 1000;

	public abstract void begin();
	public abstract void end();	
	public abstract void error();
	public abstract boolean fact(QuerySolution qs) throws InterruptedException;
	
	
	public enum SparqlStatus {
		WAIT,
		BEGIN,
		SEND,
		RECEIVE,
		END
	};
	
	public SparqlQuerier(String query, String triplestore) {
		this(query, triplestore, Integer.MAX_VALUE);
	}

	public SparqlQuerier(String query, String triplestore, int size) {
		this.query = query;
		this.triplestore = triplestore;
		this.size = size;
	}

	public void execute() throws InterruptedException {
		try {
		status = SparqlStatus.BEGIN;
		begin();
		offset = 0;
		int k = 0;
		int success = 0;
		do {
			k = 0;
			String queryStr = "" 
					+ query
					+ " LIMIT " + limit
 					+ " OFFSET " + (limit * offset);
			logger.debug("query " + triplestore + ": " + queryStr);
			status = SparqlStatus.SEND;
			Query query = QueryFactory.create(queryStr);
			try (QueryExecution qexec = QueryExecutionFactory.sparqlService(triplestore, query)) {
			//qexec.setTimeout(readTimeout, connectTimeout);
			//if (timeout != null)
				((QueryEngineHTTP) qexec).addParam("timeout", "400000");
			if (qexec != null) {
				ResultSet resultSet = qexec.execSelect();
				status = SparqlStatus.RECEIVE;
				if (resultSet != null) {
					while (resultSet.hasNext()) {
						QuerySolution qs = resultSet.nextSolution();
						if (qs != null) {
							if (success < size && fact(qs))
								success++;
							k++;
						}
					}
				}
			}
			offset++;
			}
			//System.out.println(k + " " + offset + " " + success);
		} while (k >= limit && success < size);
		status = SparqlStatus.END;
		end();
		status = SparqlStatus.WAIT;
		} catch (Exception e) {
			error();
			e.printStackTrace();
			if(e instanceof InterruptedException) {
				throw e;
			}
		}
	}
	
	public void setLimit(int limit) {
		this.limit = limit;
	}
	public void setTimeout(String timeout) {
		this.timeout = timeout;
	}
	
	public SparqlStatus getStatus() {
		return status;
	}
	
	public void safeExecute(long millis) throws InterruptedException {
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
					try {
						execute();
					} catch (InterruptedException e) {
						logger.info(e);
					}
			}
			
		});
		t.start();
		t.join(millis);
		if (getStatus() != SparqlStatus.SEND) {
			t.join();
		}
		else {
			t.interrupt();
		}
	}
}