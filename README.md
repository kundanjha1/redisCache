# redisCache
Custom Driver Class with redis as cache store

Add a config as config.properties with database properties as required in CachedDataSource.java.
Change or add database setting as required in CachedDatasource.java(VERTICA,IMPALA are already added, format should be jdbc:databasecache:)
For geeting connetion call CachedDatasource.getInstance().getConnection("<database_name>",<time_to_cache_in_seconds>).
