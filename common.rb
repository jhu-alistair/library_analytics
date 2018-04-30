# Common routines used by Kiba ETL
require 'awesome_print'
require 'csv'
require 'facets/kernel/blank'
require 'json'
require 'sequel'
require 'net/ldap'
require 'yaml'

# Display a row
def show_me!
  transform do |row|
    ap row
    row # always return the row to keep it in the pipeline
  end
end

# read in a text file holding an sql expression
#  only works for files with .sql extension
def get_sql(fname)
  fname << '.sql'
  sql = ''
  File.readlines(fname).each do |ln|
    sql << ln.chomp
  end
  sql
end

# CVS Source
class CsvSource
  def initialize(file, options)
    @file = file
    @options = options
  end

  def each
    CSV.foreach(@file, @options) do |row|
      yield row.to_hash
    end
  end
end

class DbSource
  def initialize(db_conn, sql)
    @db_conn = db_conn
    @sql = get_sql(sql)
    puts "DbSource"
    yfile = YAML.load(File.open('.config/db_connections.yaml'))
    env = yfile[:active_env]
    @db_params = yfile[@db_conn][env]
  end

  def each
    src = Sequel.connect(@db_params)
    src_ds = src[@sql]
    src_ds.each do |row|
      yield(row.to_hash)
    end
  end
end

class Anonymize
  def initialize(fld_name:)
    @fld_name = fld_name
    @ldap_key = :uid
    @ldap_key = :johnshopkinseduhopkinsid if fld_name == :hopkins_id
  end

  def process(row)
    lkup_val = row[@fld_name]
    row.delete(@fld_name)
    yfile = YAML.load(File.open('.config/ldap_connection.yaml'))
    tree = yfile['treebase']
    env = yfile['active_environment']
    ldap_config = yfile[env]
    ldap = Net::LDAP.new ldap_config
    filter = Net::LDAP::Filter.eq(@ldap_key, lkup_val)
    schema = YAML.load(File.open('jhed_schema.yaml'))
    attrs = schema.keys
    ldap.search(
      base: tree,
      filter: filter,
      attributes: attrs,
      return_result: false
    ) do |entry|
      entry.each do |attribute, values|
        value_str = values.join(',')
        row[attribute.to_sym] = value_str
      end
    end
    row.delete(:dn)
    row
  end
end

# Yep
def validate_field_values (fld_value, fld_params)
  if fld_params
    case fld_params[:data_type]
      when "integer_fld"
        return fld_value.to_i
      when "float_fld"
        return fld_value.to_f
      when "string_fld"
        return fld_value.to_s[0, fld_params[:max_length]]
      when "date_fld"
        return fld_value.to_date
      else
        return fld_value
    end
  else
    return fld_value
  end
end



class VerifyFieldsPresence
  def initialize(expected_fields)
    @expected_fields = expected_fields
  end

  def process(row)
    @expected_fields.each do |field|
      if row[field].blank?
        raise "Row lacks value for field #{field} - #{row.inspect}"
      end
    end
    row
  end
end

class RenameField
  def initialize(from:, to:)
    @from = from
    @to = to
  end

  def process(row)
    row[@to] = row.delete(@from)
    row
  end
end

# simple destination assuming all rows have the same fields
class MyCsvDestination
  attr_reader :output_file

  def initialize(output_file)
    @output_file = output_file
  end

  def write(row)
    @csv ||= CSV.open(output_file, 'w')
    unless @headers_written
      @headers_written = true
      @csv << row.keys
    end
    @csv << row.values
  end

  def close
    @csv.close
  end
end
