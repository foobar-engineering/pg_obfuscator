# frozen_string_literal: true

class Obfuscator
  def initialize(config)
    @config = config
    @dir = File.join(Dir.pwd, 'tables')
  end

  def run
    # skip tables
    @config['tables'].reject! { |table| @config['main']['tables']['skip_loading'].include?(table) }

    columns_config = YAML.load_file(File.join(Dir.pwd, 'tables', 'config.yml')).to_h

    @config['tables'].each do |table|
      handle_errors('Obfuscation tables', table: table) do
        source_file = file_path('source', table[0])
        obfuscated_file = file_path('obfuscated', table[0])
        result_file = file_path('result', table[0])

        headers = columns_config[table[0]]['headers']['source']

        structure = headers.map do |header|
          "#{header} #{table[1]['columns'][header]['obfuscator_data_type']}"
        end.join(', ')

        if @config['main']['tables']['skip_obfuscation'].include?(table[0]) || structure.empty?
          FileUtils.cp(source_file, obfuscated_file)
        else
          $log.info("Obfuscation table #{table[0]}")

          system("clickhouse-obfuscator --seed '$(head -c16 /dev/urandom | base64)' --input-format CSV --output-format CSV --structure '#{structure}' < #{source_file} > #{obfuscated_file}")
        end

        source_headers = columns_config[table[0]]['headers']['source']
        excluded_headers = columns_config[table[0]]['headers']['excluded']
        headers = source_headers + excluded_headers

        obfuscated_data = CSV.open(obfuscated_file, row_sep: "\n")
        excluded_data = CSV.open(file_path('excluded', table[0]), row_sep: "\n")

        CSV.open(result_file, 'wb') do |csv|
          csv << headers

          handle_errors('Merging files', table: table) do
            obfuscated_data.each do |row|
              row += excluded_data.readline

              row = try_fake_row(row, table, headers)

              csv << row
            end
          end
        end

        obfuscated_data.close
        excluded_data.close
      end
    end
  end

  private

  def try_fake_row(row, table, headers)
    fake_data_columns = @config['tables'][table[0]]['columns'].select { |_, f| f['fake_data'] }

    fake_data_columns.each do |column|
      header_index = headers.index(column[0])
      row[header_index] =
        case column[1]['fake_data']['type']
        when 'pattern'
          hash_row = Hash[headers.map(&:to_sym).zip(row)]
          DataFaker::General.pattern(column[1]['fake_data']['value'], hash_row)
        when 'precise'
          column[1]['fake_data']['value']
        when 'method'
          DataFaker.call_method(column[1]['fake_data']['value'])
        end
    end

    row
  end

  def file_path(sub_dir, name, ext: 'csv')
    path = create_dir(sub_dir)

    File.join(path, "#{name}.#{ext}")
  end

  def create_dir(sub_dir)
    path = File.join(@dir, sub_dir)
    FileUtils.mkdir_p(path)
  end
end
