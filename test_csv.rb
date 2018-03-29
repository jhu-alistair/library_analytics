require 'csv'
require 'mysql2'
require 'sequel'
require 'yaml'

yf_env = YAML.load(File.open('environment.yaml'))
env = yf_env[:environment]
db_conn = yf_env[:db_connection][env]
DB = Sequel.connect db_conn
@ds = DB[:ga_referrals]

# Yaml file with info on db tables and import files
import_params = YAML.load(File.open('file_to_table.yaml'))

# TARGET TABLE LOOP
import_params[:target_tables].each do |target_table, import_params|
  path_and_pattern = import_params[:source_file][:source_dir] + import_params[:source_file][:file_pattern]
  comment_regex = Regexp.new(import_params[:source_file][:comment_regex]) unless import_params[:source_file][:comment_regex].nil?
  source_cols = Array[]
  target_fields = Array[]
  import_params[:target_fields].each do |fld, params|
    unless params.nil? || params[:map_to_col].nil?
      target_fields.push fld
      source_cols.push params[:map_to_col]
    end
  end
  puts "Target fields: #{target_fields}"
  puts "Source columns: #{source_cols}"

  # SOURCE FILE LOOP
  # Dir.glob(path_and_pattern).each do |source_file|
  #   csv_source = CSV.open(source_file, 'r:utf-8', headers: true, skip_blanks: true, skip_lines: comment_regex)
  #   csv_source.shift
  #   puts csv_source.headers


    # csv_source.each do |row|
    #   puts row['Sessions']
    #   p Sequel[row['Sessions']].cast_numeric
    # end
  # end
end


#     mycsv = CSV.open(@file_name, 'r:utf-8', headers: true, skip_blanks: true, skip_lines: comment_rgx)
#     mycsv.each do |row|
#       row.each do |key, value|
#         puts "Key: #{key}    Value: #{value.delete(',')}"
#     # @ds.insert(name: 'Sharon', grade: 50)
#       end
#     end
#   end
# end
