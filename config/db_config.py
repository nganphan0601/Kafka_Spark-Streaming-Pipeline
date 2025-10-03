DB_CONFIG = {
    "host": "localhost",           
    "database": "spark_kafka_project",
    "user": "postgres",
    "password": "06012002",
}

JDBC_URL = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

JDBC_PROPERTIES = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver"
}
