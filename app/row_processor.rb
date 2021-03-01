class RowProcessor
  class << self
    attr_accessor :config, :columns, :row

    def run(config:, columns:, row:)
      @config = config
      @columns = columns
      @row = row

      return if config['main']['tables']['skip_loading'].include?(row['table_name'])

      update_reasons!
      update_data_type!
      exclude
      mark_to_fix
    end

    def update_reasons!
      reasons = []
      reasons << 'unknown_data_type' if unknown_data_type?
      reasons << row['constraint_type'] if constraint?
      reasons << 'unique_index' if unique_index?
      reasons << 'indexing_few_columns' if indexing_few_columns?

      row['reasons'] = reasons
    end

    def update_data_type!
      config['tables'][row['table_name']]['columns'][row['column_name']].tap do |column|
        column['db_data_type'] = row['data_type']
        column['not_null'] = row['not_null']
        next if unknown_data_type? || fake_data?

        column['obfuscator_data_type'] = obfuscator_data_type
      end
    end

    def exclude
      if primary_key? || (foreign_key? && foreign_column_primary? && !indexing_few_columns?)
        config['tables'][row['table_name']]['excluded_columns'] << row['column_name']
        config['tables'][row['table_name']]['excluded_columns'].uniq!
      end
    end

    def mark_to_fix
      config['tables'][row['table_name']]['columns'][row['column_name']].delete('need_fix')

      return if row_excluded? || table_excluded?
      return if fake_data?
      return if row['reasons'].empty?

      row['need_fix'] = true
      config['tables'][row['table_name']]['columns'][row['column_name']]['need_fix'] = true
    end

    private

    def obfuscator_data_type
      data_type = config['main']['data_types_map'][row['data_type']]
      row['not_null'] ? data_type : "Nullable(#{data_type})"
    end

    def fake_data?
      config['tables'][row['table_name']]['columns'][row['column_name']].key?('fake_data')
    end

    def unknown_data_type?
      !config['main']['data_types_map'][row['data_type']]
    end

    def indexing_few_columns?
      row['is_index'] && row['indexing_ids'].size > 1
    end

    def unique_index?
      row['is_unique_index']
    end

    def constraint?
      !!row['constraint_type']
    end

    def primary_key?
      row['constraint_type'] == 'primary_key'
    end

    def foreign_key?
      row['constraint_type'] == 'foreign_key'
    end

    def foreign_column_primary?
      columns.any? do |column|
        column['table_name'] == row['foreign_table'] &&
          column['column_name'] == row['foreign_column']
      end
    end

    def row_excluded?
      config['tables'][row['table_name']]['excluded_columns'].include?(row['column_name'])
    end

    def table_excluded?
      config['main']['tables']['skip_obfuscation'].include?(row['table_name'])
    end
  end
end