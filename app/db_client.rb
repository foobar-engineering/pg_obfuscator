# frozen_string_literal: true

require 'csv'

class DBClient
  attr_reader :psql_params, :pg_params, :schema

  def initialize(params = {})
    dbname = params.fetch(:dbname)
    user = params.fetch(:user)
    host = params.fetch(:host, '0.0.0.0')
    port = params.fetch(:port, 5432)
    password = params.fetch(:password, 'password')
    @schema = params[:schema] || 'public'

    @psql_params = "PGPASSWORD=#{password} psql -U #{user} -h #{host} -p #{port} -d #{dbname}"
    @pg_params = { host: host, port: port, user: user, password: password, dbname: dbname }
    @pgdump_params = "PGPASSWORD=#{password} pg_dump -U #{user} -h #{host} -p #{port} -d #{dbname} -n #{schema}"
  end

  # Returns Array of Hashes:
  #
  # id - number of column in pg_attribute
  # table_name
  # column_name
  # data_type
  # not_null
  # indexing_ids
  # index_statement_def
  # is_index
  # is_unique_index
  # constraint_type
  # foreign_table
  # foreign_column
  #
  def get_columns
    query = <<-SQL.gsub("\n", '')
      SELECT
        f.attnum AS id,
        c.relname::text AS table_name,
        f.attname AS column_name,
        t.typname AS data_type,
        f.attnotnull AS not_null,
        ix.indkey::int[] AS indexing_ids,
        ixs.indexdef as index_statement_def,
        CASE
          WHEN i.oid<>0 THEN true
          ELSE false
        END AS is_index,
        ix.indisunique AS is_unique_index,
        CASE p.contype
          WHEN 'p' THEN 'primary_key'
          WHEN 'f' THEN 'foreign_key'
          WHEN 'u' THEN 'unique_key'
          WHEN 'c' THEN 'check_key'
          WHEN 'x' THEN 'exclusion_key'
          ELSE NULL
        END AS constraint_type,
        g.relname as foreign_table,
        fa.attname as foreign_column
      FROM pg_attribute f
        JOIN pg_class c ON c.oid = f.attrelid
        JOIN pg_type t ON t.oid = f.atttypid
        LEFT JOIN pg_attrdef d ON d.adrelid = c.oid
          AND d.adnum = f.attnum
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_constraint p ON p.conrelid = c.oid
          AND f.attnum = ANY (p.conkey)
        LEFT JOIN pg_class AS g ON p.confrelid = g.oid
        LEFT JOIN pg_attribute AS fa ON fa.attrelid = c.oid
          AND fa.attnum = ANY(p.confkey)
        LEFT JOIN pg_index AS ix ON f.attnum = ANY(ix.indkey)
          AND c.oid = f.attrelid
          AND c.oid = ix.indrelid
        LEFT JOIN pg_class AS i ON ix.indexrelid = i.oid
        LEFT JOIN pg_indexes AS ixs ON ixs.indexname = i.relname
      WHERE c.relkind = 'r'::char
        AND n.nspname = '#{@schema}'
        AND f.attnum > 0
      ORDER BY c.relname, f.attnum;
    SQL

    send_query_pg(query)
  end

  def get_sequences
    query = <<-SQL.gsub("\n", '')
      SELECT seqclass.relname AS sequence_name,
        depclass.relname AS table_name,
        attrib.attname   AS column_name
      FROM   pg_class AS seqclass
        JOIN pg_sequence AS seq
          ON (seq.seqrelid = seqclass.relfilenode)
        JOIN pg_depend AS dep
          ON (seq.seqrelid = dep.objid)
        JOIN pg_class AS depclass
          ON (dep.refobjid = depclass.relfilenode)
        JOIN pg_attribute AS attrib
          ON (attrib.attnum = dep.refobjsubid
        AND attrib.attrelid = dep.refobjid)
    SQL

    result = exec(query)
    result.shift && result
  end

  def update_sequences(sequence_name, table_name, column_name)
    query = <<-SQL.gsub("\n", '')
        SELECT setval('#{sequence_name}', (SELECT MAX(#{column_name}) FROM #{table_name}));
    SQL

    $log.info("Updating sequence #{sequence_name}, table: #{table_name}, column: #{column_name}")

    exec(query)
  end

  def get_table_data(table:, columns:, output:)
    query = <<-CMD.gsub("\n", '').lstrip
      \\copy (SELECT #{columns.join(', ')} FROM #{table} #{'ORDER BY id' if columns.include?('id')})
        TO '#{output}' WITH CSV NULL '\\N' #{force_quote_columns(table, columns)};
    CMD
    exec(query, format: 'raw')

    $log.info("Table #{table} saved to #{output}")
  end

  def insert_tables_from_file(table:, headers:, input:)
    query = <<-CMD.gsub("\n", '').lstrip
      \\copy #{table}(#{headers.join(',')}) FROM '#{input}' WITH NULL '\\N' CSV header;
    CMD
    exec(query, format: 'raw')

    $log.info("Table #{table} loaded from #{input}")
  end

  def insert_schema(input)
    raise "File #{input} not exists" unless File.file?(input)

    `#{@psql_params} -f #{input}`

    $log.info('Schema loaded')
  end

  def prepare_field(field, type)
    case type
    when 'timestamp'
      "to_char(#{field}, 'YYYY-MM-DD hh:mm:ss') as #{field}"
    else
      field
    end
  end

  def dump_schema(output:, section:)
    `#{@pgdump_params} -f #{output} --section=#{section}`

    $log.info("Schema (section: #{section}) saved to #{output}")
  end

  def table_exist?(table_name)
    query = <<-SQL
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE  table_schema = '#{@schema}'
            AND    table_name   = '#{table_name}'
        )
    SQL

    response = send_query_pg(query)
    response[0]['exists']
  end

  private

  def exec(query, format: 'csv')
    # Build query:
    # -F            --- field-separator output ','
    # -A            --- unaligned table output mode
    # -c            --- run only single command end exit
    # -P footer=off --- disable output rows count
    #
    query = " -F , -Ac \"#{query}\" -P footer=off"
    response = send_query(query)

    format == 'raw' ? response : CSV.parse(response)
  end

  def send_query(query)
    $log.debug("Sending query:\n#{query}")

    `#{@psql_params + query}`
  end

  def force_quote_columns(table, columns)
    @config ||= Config.load

    force_quote = @config['tables'][table]['columns'].select do |column, data_types|
      columns.include?(column) && data_types['obfuscator_data_type']&.include?('String')
    end.keys

    "FORCE QUOTE #{force_quote.join(',')}" unless force_quote.empty?
  end

  def send_query_pg(query)
    connect = PG.connect(@pg_params)
    connect.type_map_for_results = PG::BasicTypeMapForResults.new(connect)
    connect.exec(query).to_a
  end
end
