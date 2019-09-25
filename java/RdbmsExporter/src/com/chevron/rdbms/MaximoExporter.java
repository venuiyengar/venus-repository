package com.chevron.rdbms;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

public class MaximoExporter {
	
	private PoolDataSource pds;
	private Properties props;
	
	private String jdbcConnection;
	private String jdbcUsername;
	private String jdbcPassword;
	
	private String restScheme;
	private String restHost;
	private String restPort;
	private String restUsername;
	private String restPassword;
	
	private int jdbcPauseBetweenTargets;
	private int jdbcPauseBetweenRows;	

	public static void main(String[] args) {
		System.out.println("Maxiomo Exporter Starting...");
		
		 
		MaximoExporter exporter = new MaximoExporter();
		try {
			exporter.init();
			String dataSource = exporter.getDataSource();
			List<String> targets = exporter.getTargets();
			for(String target : targets){
				int startRow = exporter.getLastIndex(dataSource, target);
				int totalRows = exporter.getTotals(target);
				
				if(totalRows > startRow) {
					System.out.print("\nStarting "+ dataSource+ ":"+ target+" row "+ startRow +" to "+totalRows);
				} else {
					System.out.print("\n"+ dataSource+ ":"+ target+" is current");
				}
		
				for(int i = startRow; i <=totalRows; i++){
					String xml = exporter.executeQuery(target, i);
					System.out.println("DOC["+i+"]:" + xml);
					exporter.ingest(dataSource, target, xml);
					if(i%10==0) System.out.print(".");
					exporter.sleepBetweenRows();
				}
				exporter.sleepBetweenTargets();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException sqe){
			sqe.printStackTrace();
		}

		System.out.println("\n\nMaximo Exporter Finished.");
	}
	public MaximoExporter(){}

	private void init() throws SQLException, IOException{
		initProperties();
		initPoolDataSource();
	}
	
	private void initProperties() throws IOException{
		Properties props = new Properties();
		String path = "./data-source.properties";
		FileInputStream file = new FileInputStream(path);
		props.load(file);
		file.close();
		
		this.jdbcConnection = props.getProperty("JDBC_CONNECTION");
		this.jdbcUsername = props.getProperty("JDBC_USERNAME");
		this.jdbcPassword = props.getProperty("JDBC_PASSWORD");
		this.restUsername = props.getProperty("REST_USERNAME");
		this.restPassword = props.getProperty("REST_PASSWORD");
		this.restScheme = props.getProperty("REST_SCHEME");
		this.restHost = props.getProperty("REST_HOST");
		this.restPort = props.getProperty("REST_PORT");
		
		this.jdbcPauseBetweenTargets = Integer.valueOf(props.getProperty("PAUSE_BETWEEN_TARGETS"));
		this.jdbcPauseBetweenRows = Integer.valueOf(props.getProperty("PAUSE_BETWEEN_ROWS"));
		
		// sanity check values?
		this.props = props;
	}
	
	private void initPoolDataSource() throws SQLException{
		  PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
		  pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
		  pds.setURL(this.jdbcConnection);
		  pds.setUser(this.jdbcUsername);
		  pds.setPassword(this.jdbcPassword);
		  //Setting pool properties, will want to tweak these settings
		  pds.setInitialPoolSize(5);
		  pds.setMinPoolSize(5);
		  pds.setMaxPoolSize(10);
		  this.pds = pds;
	}
	
	private String executeQuery(String target, int currentRow){
		String xml = null;
		
		// catch the missing/bad props
		String targetQuery = this.props.getProperty(target+".query");
		String fields = this.props.getProperty(target+".fields");
		
		try(Connection con = this.pds.getConnection();
			PreparedStatement ps = createPreparedStatement(con,targetQuery,currentRow);
			ResultSet rs = ps.executeQuery();)
		{
			xml = genXml(rs, fields);
			
		}catch (SQLException e) {

			System.err.println("Database issue!");
			
			if(12519 == e.getErrorCode()){
				System.err.println("Listener refused connection - pausing a bit, trying again");
				try {
				   TimeUnit.MILLISECONDS.sleep(500);
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			} else {
				e.printStackTrace();
			}
		}
		return xml;
	}
	
	private int getTotals(String target){
		int totalRows = 0;
		// catch the missing/bad props
		String targetTotal = this.props.getProperty(target+".total");
		
		try(Connection con = this.pds.getConnection();
			PreparedStatement ps = getTotalPreparedStatement(con, targetTotal);
			ResultSet rs = ps.executeQuery();)
		{	
			while(rs.next()) {
		   	totalRows = rs.getInt("NUM_ROWS");
		   }
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return totalRows;
	}
	
	
	private static String genXml(ResultSet rs, String fields) throws SQLException{
		List<String> targetFields = new ArrayList<String>();
		StringTokenizer tokens = new StringTokenizer(fields, ",");
		while(tokens.hasMoreTokens()){
			targetFields.add(tokens.nextToken());
		}
		
		StringBuilder xml = new StringBuilder();
	   while(rs.next()) {
	   	xml = new StringBuilder();
			xml.append("<resource>");
			for(String field : targetFields){
				xml.append("<");
				xml.append(StringEscapeUtils.escapeXml11(field));
				xml.append(">");
				xml.append(StringEscapeUtils.escapeXml11((rs.getString(field))));
				xml.append("</");
				xml.append(StringEscapeUtils.escapeXml11(field));
				xml.append(">");
			}
			xml.append("</resource>");
	   }
	   return xml.toString();
	}
	
	private String getDataSource(){
		String dataSource = this.props.getProperty("DATA_SOURCE");
		return dataSource;
	}
	
	private List<String> getTargets(){
		String targetsProperty = this.props.getProperty("TARGETS");
		List<String> targets = new ArrayList<String>();
		StringTokenizer tokens = new StringTokenizer(targetsProperty, ",");
		while(tokens.hasMoreTokens()){
			targets.add(tokens.nextToken());
		}
		return targets;
	}

	private PreparedStatement getTotalPreparedStatement(Connection connection, String sql) throws SQLException{
		PreparedStatement ps = connection.prepareStatement(sql);
		return ps;
	}
	
	private PreparedStatement createPreparedStatement(Connection connection, String innerSql, int currentRow) throws SQLException {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * FROM (SELECT a.*, rownum rnum FROM (");
		sql.append(innerSql);
		sql.append(") a WHERE rownum <= ? ) WHERE rnum >= ?");
		
	   PreparedStatement ps = connection.prepareStatement(sql.toString());
	   ps.setInt(1, currentRow);
	   ps.setInt(2, currentRow);
	   return ps;
	}
	
	private void ingest(String dataSource, String target, String xml){
		StringBuilder restUri = new StringBuilder();
		restUri.append(this.restScheme);
		restUri.append("://");
		restUri.append(this.restHost);
		restUri.append(":");
		restUri.append(this.restPort);
		restUri.append("/v1/resources/ingest-clinicopia");

		AuthScope authScope = new AuthScope(this.restHost, AuthScope.ANY_PORT);
		UsernamePasswordCredentials userCredentials = new UsernamePasswordCredentials(this.restUsername, this.restPassword);
		
		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(authScope, userCredentials);
		
		// need to rework, since some is not auto closable...
		try(CloseableHttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(credentialsProvider).build();)
		{
			HttpPost httppost = new HttpPost(restUri.toString());
			
    		List<NameValuePair> params = new ArrayList<NameValuePair>(2);
    		params.add(new BasicNameValuePair("rs:dataSource", dataSource));
    		params.add(new BasicNameValuePair("rs:target", target));
    		params.add(new BasicNameValuePair("rs:xml", xml));
    		httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

			CloseableHttpResponse response = httpClient.execute(httppost);
			int statusCode = response.getStatusLine().getStatusCode();
			if(HttpStatus.SC_CREATED == statusCode || HttpStatus.SC_OK == statusCode){
//				System.out.println("*** RESPONSE: "+ EntityUtils.toString((response.getEntity())));
			} else {
				// something went horribly wrong. 
				System.err.println("Unable to connect to MarkLogic REST endpoint: "+ restUri);
				System.err.println("*** HTTP Status : "+ response.getStatusLine());
				System.err.println("*** Response    :\n" + EntityUtils.toString((response.getEntity())));
			}
			response.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private int getLastIndex(String dataSource, String target){
		int lastIndex = -1;
		
		StringBuilder restUri = new StringBuilder();
		restUri.append(this.restScheme);
		restUri.append("://");
		restUri.append(this.restHost);
		restUri.append(":");
		restUri.append(this.restPort);
		restUri.append("/v1/resources/get-state?rs:dataSource=");
		restUri.append(dataSource);
		restUri.append("&rs:target=");
		restUri.append(target);

		AuthScope authScope = new AuthScope(this.restHost, AuthScope.ANY_PORT);
		UsernamePasswordCredentials userCredentials = new UsernamePasswordCredentials(this.restUsername, this.restPassword);
		
		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(authScope, userCredentials);
		
		// need to rework, since some is not auto closable...
		try(CloseableHttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(credentialsProvider).build();)
		{
			HttpGet httpGet = new HttpGet(restUri.toString());
			CloseableHttpResponse response = httpClient.execute(httpGet);
			int statusCode = response.getStatusLine().getStatusCode();
			if(HttpStatus.SC_CREATED == statusCode || HttpStatus.SC_OK == statusCode){
				lastIndex = Integer.valueOf(EntityUtils.toString((response.getEntity())));
			} else {
				// something went horribly wrong. 
				System.err.println("Unable to connect to MarkLogic REST endpoint: "+ restUri);
				System.err.println("*** HTTP Status : "+ response.getStatusLine());
				System.err.println("*** Response    :\n" + EntityUtils.toString((response.getEntity())));
			}
			response.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lastIndex;
	}
	
	private void sleepBetweenTargets(){
		sleep(this.jdbcPauseBetweenTargets);
	}
	
	private void sleepBetweenRows(){
		sleep(this.jdbcPauseBetweenRows);
	}
	
	private void sleep(int ms){
		try {
		   TimeUnit.MILLISECONDS.sleep(ms);

		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
	}
}
