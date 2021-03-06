# Common routines used by Kiba ETL
require 'awesome_print'
require 'csv'
require 'facets/kernel/blank'
require 'json'
require 'mysql2'
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
  def initialize(db_conn:, sql_file:)
    yfile = YAML.load(File.open('.config/db_connections.yaml'))
    env = yfile[:active_env]
    src = Sequel.connect(yfile[db_conn][env])
    @src_ds = src[get_sql(sql_file)]
  end

  def each
      @src_ds.each do |row|
        begin
          yield(row.to_hash)
        rescue Sequel::Error
          puts $ERROR_INFO.message
          ap row
        end
      end
  end
end

class AppendToDB
  def initialize(db_conn:, db_table:)
    yfile = YAML.load(File.open('.config/db_connections.yaml'))
    env = yfile[:active_env]
    trgt = Sequel.connect(yfile[db_conn][env])
    @trgt_ds = trgt[db_table]
  end

  def write(row)
    begin
      @trgt_ds.insert(row)
    rescue Sequel::Error
      puts $ERROR_INFO.message
      ap row
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
        row[attribute.to_sym] = value_str unless attribute == :dn
      end
    end
    row
  end
end

class ValidateValues
  def initialize(schema:)
    @schema = schema
  end

  def process(row)
    @schema.each do |fld, params|

      fld_val = row[fld]

      if (fld_val && params)
        case params[:data_type]
        when 'integer_fld'
          row[fld] = fld_val.to_i
        when 'float_fld'
          row[fld] = fld_val.to_f
        when 'string_fld'
          row[fld] = fld_val.to_s[0, params[:max_length]]
          row[fld].encode!(params[:encoding], :invalid => :replace, :undef => :replace, :replace => "?") if params[:encoding]
        when 'date_fld'
          row[fld] = fld_val.to_date
        end
      end
    end
    row
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

class RenameFields
  def initialize(schema:)
    @ren = {}
    schema.each do |key, value|
      @ren[key] = value[:rename] if value.keys.include?(:rename)
    end
  end

  def process(row)
    @ren.each do |old_key, new_key|
      row[new_key.to_sym] = row.delete(old_key)
    end
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
