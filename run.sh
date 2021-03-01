bundle exec ruby pg_obfuscator.rb --debug --configure --export-schema --export-tables --obfuscate \
--source-db-host $POSTGRES_HOST --source-db-port $POSTGRES_PORT --source-db-name $POSTGRES_DB_NAME \
--source-db-user $POSTGRES_USER --source-db-password $POSTGRES_PASSWORD \
--import \
--target-db-host $TARGET_POSTGRES_HOST --target-db-port $TARGET_POSTGRES_PORT --target-db-name $TARGET_POSTGRES_DB_NAME \
--target-db-user $TARGET_POSTGRES_USER \
--target-db-password $TARGET_POSTGRES_PASSWORD
