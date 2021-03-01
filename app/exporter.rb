# frozen_string_literal: true

class Exporter
  attr_reader :db_client, :tables_config, :config

  def initialize(db_client:, config:)
    @db_client = db_client
    @tables_config = Hash.new { |h, k| h[k] = Hash.new(&h.default_proc) }
    @config = config
  end

  def run
    tables = config['tables']
    tables.each do |table|
      handle_errors('Export tables', table: table) do
        table_name = table[0]
        next if skip_loading?(table_name)

        # Exclude columns from obfuscation
        columns = split_columns(table)

        # Prepare columns for obfuscation (transform types)
        #
        # Example:
        # 2017-05-16 13:04:32.40547 => 2017-05-16 13:04:32
        prepared_columns = columns[:columns].map do |k, v|
          db_client.prepare_field(k, v['db_data_type'])
        end

        tables_config[table_name]['headers'].tap do |h|
          h['excluded'] = columns[:excluded_columns]
          h['source'] = columns[:columns].keys
        end

        db_client.get_table_data(
          table: table_name,
          columns: prepared_columns,
          output: File.join(output_data_dir, "#{table_name}.csv")
        )
        db_client.get_table_data(
          table: table_name,
          columns: columns[:excluded_columns],
          output: File.join(excluded_data_dir, "#{table_name}.csv")
        )
      end
    end

    $log.info('Tables exported')
  end

  def save
    File.open(File.join(tables_dir, 'config.yml'), 'w+') do |f|
      f.write(tables_config.to_yaml)
    end
  end

  private

  def skip_loading?(table_name)
    config['main']['tables']['skip_loading'].include?(table_name)
  end

  def split_columns(table)
    excluded_columns = table[1]['excluded_columns']

    fake_columns = table[1]['columns'].select do |_, fields|
      fields.key?('fake_data')
    end.keys

    excluded_columns.append(*fake_columns).uniq!
    # Columns for obfuscation
    columns = (table[1]['columns'].keys - excluded_columns)
    columns = table[1]['columns'].select { |column| columns.include?(column) }

    {
      excluded_columns: excluded_columns,
      columns: columns
    }
  end

  def tables_dir
    @tables_dir ||= File.join(Dir.pwd, 'tables')
  end

  def output_data_dir
    return @output_data_dir if defined?(@output_data_dir)

    @output_data_dir = File.join(tables_dir, 'source')

    FileUtils.mkdir_p(@output_data_dir)

    @output_data_dir
  end

  def excluded_data_dir
    return @excluded_data_dir if defined?(@excluded_data_dir)

    @excluded_data_dir = File.join(tables_dir, 'excluded')

    FileUtils.mkdir_p(@excluded_data_dir)

    @excluded_data_dir
  end
end
