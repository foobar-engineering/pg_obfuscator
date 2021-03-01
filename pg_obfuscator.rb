# frozen_string_literal: true

require 'csv'
require 'yaml'
require 'optparse'
require 'logger'
require 'pg'
require 'pry'
require 'fileutils'
require_relative './app/config'
require_relative './app/data_faker'
require_relative './app/db_client'
require_relative './app/debugger'
require_relative './app/exporter'
require_relative './app/obfuscator'

include Debugger

begin
  options = {}
  parser = OptionParser.new do |opts|
    opts.banner = 'Usage: ruby pg_obfuscator.rb [--configure --export-schema --export-tables --obfuscate --import] [options]'

    opts.on('--configure', 'Generate config based on Data from CSV') do |configure|
      options[:cofigure] = configure
    end

    opts.on('--export-tables', 'Download data from each table defined in config file') do |export_tables|
      options[:export_tables] = export_tables
    end

    opts.on('--export-schema', 'Download schema from database') do |export_schema|
      options[:export_schema] = export_schema
    end

    opts.on('--obfuscate', 'Obfuscate data from input directory files and save to output directory') do |obfuscate|
      options[:obfuscate] = obfuscate
    end

    opts.on('--import', 'Import data from CSV files to database') do |import|
      options[:import] = import
    end

    opts.on('--debug', 'Enable debug mode') do |debug_mode|
      options[:debug_mode] = debug_mode
      $debug_mode = debug_mode
    end

    opts.on('--source-db-host <ip>', String, 'Database host for config generation') do |source_db_host|
      options[:source_db_host] = source_db_host
    end

    opts.on('--source-db-port <port>', String, 'Database port for config generation') do |source_db_port|
      options[:source_db_port] = source_db_port
    end

    opts.on('--source-db-name <name>', String, 'Database name for config generation') do |source_db_name|
      options[:source_db_name] = source_db_name
    end

    opts.on('--source-db-user <username>', String, 'Database user for config generation') do |source_db_user|
      options[:source_db_user] = source_db_user
    end

    opts.on('--source-db-password <password>', String,
            'Database password for config generation') do |source_db_password|
      options[:source_db_password] = source_db_password
    end

    opts.on('--source-db-schema <schema>', String, 'Database schema for source db') do |source_db_schema|
      options[:source_db_schema] = source_db_schema
    end

    opts.on('--target-db-host <ip>', String, 'Database host for import data') do |target_db_host|
      options[:target_db_host] = target_db_host
    end

    opts.on('--target-db-port <port>', String, 'Database port for import data') do |target_db_port|
      options[:target_db_port] = target_db_port
    end

    opts.on('--target-db-name <name>', String, 'Database name for import data') do |target_db_dbname|
      options[:target_db_name] = target_db_dbname
    end

    opts.on('--target-db-user <username>', String, 'Database user for import data') do |target_db_user|
      options[:target_db_user] = target_db_user
    end

    opts.on('--target-db-password <password>', String, 'Database password for import data') do |target_db_password|
      options[:target_db_password] = target_db_password
    end

    opts.on('--target-db-schema <schema>', String, 'Database schema for target db') do |target_db_schema|
      options[:target_db_schema] = target_db_schema
    end
  end

  parser.parse!
rescue OptionParser::MissingArgument => e
  puts e.message
  exit 1
end

$log = Logger.new($stdout)
$log.level = options[:debug_mode] ? Logger::DEBUG : Logger::INFO

# Initialize client for source database
if options[:cofigure] || options[:export_schema] || options[:export_tables]
  source_db_client = DBClient.new(
    dbname: options[:source_db_name],
    host: options[:source_db_host],
    port: options[:source_db_port],
    user: options[:source_db_user],
    password: options[:source_db_password],
    schema: options[:source_db_schema]
  )
end

# Initialize client for target database
if options[:import]
  target_db_client = DBClient.new(
    dbname: options[:target_db_name],
    host: options[:target_db_host],
    port: options[:target_db_port],
    user: options[:target_db_user],
    password: options[:target_db_password],
    schema: options[:target_db_schema]
  )
end

if options[:cofigure]
  Config.build(columns: source_db_client.get_columns)
  Config.save
end

if options[:export_schema]
  source_db_client.dump_schema(output: File.join(Dir.pwd, 'pre-data.sql'), section: 'pre-data')
  source_db_client.dump_schema(output: File.join(Dir.pwd, 'post-data.sql'), section: 'post-data')
end

if options[:export_tables]
  config = Config.load
  exit 1 unless Config.valid?
  exporter = Exporter.new(db_client: source_db_client, config: config)
  exporter.run
  exporter.save
end

if options[:obfuscate]
  config = Config.load
  exit 1 unless Config.valid?
  obfuscator = Obfuscator.new(config)
  obfuscator.run
end

if options[:import]
  Config.load
  exit 1 unless Config.valid?
  columns_config = YAML.load_file(File.join(Dir.pwd, 'tables', 'config.yml')).to_h
  tables = columns_config.keys

  if target_db_client.table_exist?(tables.first)
    $log.info('Tables already imported!')
    $log.info('Exit')
    exit
  end

  # Import pre-data
  target_db_client.insert_schema(File.join(Dir.pwd, 'pre-data.sql'))

  # Import tables
  tables.each do |table|
    handle_errors('Import tables', table) do
      input = File.join(Dir.pwd, 'tables', 'result', "#{table}.csv")
      headers = columns_config[table]['headers']['source'] + columns_config[table]['headers']['excluded']
      target_db_client.insert_tables_from_file(table: table, headers: headers, input: input)
    end
  end

  # Import post-data
  target_db_client.insert_schema(File.join(Dir.pwd, 'post-data.sql'))

  # Update sequences
  target_db_client.get_sequences.each { |s| target_db_client.update_sequences(*s) }

  $log.info('Import completed')
end
