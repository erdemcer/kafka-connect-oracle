package com.ecer.kafka.connect.oracle;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorSQL {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorSQL.class);

	private static final Charset SQL_FILE_ENCODING = Charset.forName("UTF8");
	private static final Map<String, String> RESOURCE_FS_ENV = new HashMap<>();

	private static final String PROPERTIES_FILE = "sql.properties";

	private static final String SQL_DICTIONARY = "dictionary";
	private static final String SQL_DICTIONARY_CDB = "dictionary.cdb";

	public static final String PARAMETER_OWNER = "owner";
	public static final String PARAMETER_TABLE_NAME = "tableName";

	private final Map<String, String> sqlResources = new HashMap<String, String>();

	public ConnectorSQL() {
		try {
			Properties sqlResourceFiles = new Properties();
			InputStream is = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE);
			sqlResourceFiles.load(is);

			FileSystem fs = null;
			for (String sqlResourceFileKey : sqlResourceFiles.stringPropertyNames()) {
				URI sqlResourceFileURI = ClassLoader.getSystemResource(sqlResourceFiles.getProperty(sqlResourceFileKey))
						.toURI();
				final String[] array = sqlResourceFileURI.toString().split("!");
				if (fs == null) {
					fs = FileSystems.newFileSystem(URI.create(array[0]), RESOURCE_FS_ENV);
				}
				final Path path = fs.getPath(array[1]);

				byte[] sql = Files.readAllBytes(path);
				sqlResources.put(sqlResourceFileKey, new String(sql, SQL_FILE_ENCODING));
			}
			if (fs != null && fs.isOpen()) {
				fs.close();
			}
		} catch (Exception e) {
			throw new IllegalStateException("Cannot initialize kafka-connect-oracle SQL resources", e);
		}
	}

	public String getDictionarySQL() {
		return sqlResources.get(SQL_DICTIONARY);
	}

	public String getContainerDictionarySQL() {
		return sqlResources.get(SQL_DICTIONARY_CDB);
	}
}
