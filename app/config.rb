# frozen_string_literal: true

require 'yaml'
require_relative 'row_processor'
class Config
  class << self
    attr_accessor :config

    DEFAULT_CONFIG_PATH = 'config/config.yml'
    CUSTOM_CONFIG_PATH = 'config/custom_config.yml'

    # Predefined data types transformation map for obfuscation
    DATA_TYPES = {
      'citext' => 'String',
      'inet' => 'IPv4',
      'int2' => 'Int8',
      'int4' => 'Int8',
      'int8' => 'Int8',
      'varchar' => 'String',
      'timestamp' => 'DateTime',
      'text' => 'String'
    }.freeze

    def build(columns:)
      custom_config = self.load(CUSTOM_CONFIG_PATH)
      @columns = columns

      $log.info('Building config')

      @columns_to_fix = []

      @config = Hash.new { |h, k| h[k] = Hash.new(&h.default_proc) }
      @config = fill_default(@config)
      @config = merge_custom_to_default(@config, custom_config)

      @columns.each do |row|
        if @config['tables'][row['table_name']]['excluded_columns'] == {}
          @config['tables'][row['table_name']]['excluded_columns'] = []
        end

        RowProcessor.run(config: @config, columns: @columns, row: row)
        @columns_to_fix << row if row['need_fix']
      end

      @config = merge_custom_to_default(@config, custom_config)

      show_columns_to_fix unless valid?(log: false)

      $log.info("Processed #{@config['tables'].size} tables")
      $log.info('Check config before run export tables and obfuscation!')
      self
    end

    def load(config = DEFAULT_CONFIG_PATH)
      $log.debug("Loading config: #{config}")

      @config = YAML.load_file(config) || {}
    rescue Errno::ENOENT
      FileUtils.mkdir_p(File.dirname(config))
      File.open(config, 'w+')
      {}
    end

    def save(output = DEFAULT_CONFIG_PATH)
      FileUtils.mkdir_p(File.dirname(output))

      File.open(output, 'w+') do |f|
        f.write @config.to_yaml
      end

      $log.info("Config saved to: #{output}")
    end

    def valid?(log: true)
      message = []

      @config['tables'].each do |table_name, opts|
        next if @config['main']['tables']['skip_loading'].include?(table_name)

        fix_columns = opts['columns'].select { |_, values| values.key?('need_fix') }
        next if fix_columns.empty?

        message << "Table: #{table_name}"
        message << "Columns: #{fix_columns.map(&:first).join(', ')}"
      end

      message.insert(0, 'You have columns waiting to fix in config:') unless message.empty?

      $log.info(message.join("\n")) if log

      return message.empty?
    end

    private

    def show_columns_to_fix
      tables = @columns_to_fix.group_by { |row| row['table_name'] }

      message = ['Excluded columns:']
      message << '-' * 10

      tables.each do |table, row|
        message << "Table: #{table}"
        message << 'Columns:'

        row.each_with_index do |r, i|
          next if !r.key?('need_fix')
          message << "#{i + 1}) #{r['column_name']}(#{r['data_type']})"
          message << 'reasons:'

          r['reasons'].each do |reason|
            reason += ": [#{r['index_statement_def']}]" if reason == 'unique_index'

            if reason == 'indexing_few_columns'
              indexing_columns = @columns.select do |c|
                r['indexing_ids'].include?(c['id']) &&
                  r['table_name'] == c['table_name']
              end.map { |c| c['column_name'] }.uniq
              unknown_column = ', <unknown_column>' if r['indexing_ids'].include?(0)
              reason += ": [#{indexing_columns.join(', ')}#{unknown_column}]"
            end

            message << "- #{reason}"
          end
        end

        message << '-' * 10
      end

      message << "Tables needs to manual fix: #{@columns_to_fix.map { |r| r['table_name'] }.uniq.size}"
      message << "Columns: #{@columns_to_fix.size}"
      message << '-' * 10

      $log.info(message.join("\n"))
    end

    def fill_default(config)
      config['main']['data_types_map'] = DATA_TYPES
      config['main']['tables']['skip_obfuscation'] = []
      config['main']['tables']['skip_loading'] = []

      config
    end

    def merge_custom_to_default(default, custom)
      default['main'].tap do |main|
        main['data_types_map'].merge(custom['main']['data_types_map']) if custom.dig('main', 'data_types_map')

        if custom.dig('main', 'tables', 'skip_loading')
          main['tables']['skip_loading'].append(*custom['main']['tables']['skip_loading']).uniq!
        end

        if custom.dig('main', 'tables', 'skip_obfuscation')
          main['tables']['skip_obfuscation'].append(*custom['main']['tables']['skip_obfuscation']).uniq!
        end
      end

      custom['tables']&.each do |table_name, columns|
        if columns['excluded_columns']
          if default['tables'][table_name]['excluded_columns'] == {}
            default['tables'][table_name]['excluded_columns'] = []
          end

          default['tables'][table_name]['excluded_columns'].append(*columns['excluded_columns']).uniq!
        end

        columns['columns']&.map do |column_name, fields|
          default['tables'][table_name]['columns'][column_name].merge!(fields)
        end
      end

      default
    end
  end
end
