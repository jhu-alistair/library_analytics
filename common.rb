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

class AnonymizeHopkinsId
  def initialize
    @user_attribs = {}
  end

  def process(row)
    prfl = GetUserProfile.new
    @user_attribs = prfl.ldap_lookup('johnshopkinseduhopkinsid', row['hopkins_id'])
    row << @user_attribs
    row
  end
end

class GetUserProfile

  def initialize
    yfile = YAML.load(File.open('.config/ldap_connection.yaml'))
    @tree = yfile['treebase']
    env = yfile['active_environment']
    ldap_config = yfile[env]
    @ldap = Net::LDAP.new ldap_config
  end

  def ldap_lookup (lkup_field, lkup_val)
    # Add test for lkup_field is JHED (uid) or Hopkins ID
    filter = Net::LDAP::Filter.eq(lkup_field, lkup_val)
    user = {}
    schema = YAML.load(File.open('jhed_profile_schema.yaml'))
    attrs = schema.keys
    ap attrs  #DEBUGGING
    @ldap.search(
      base: @tree,
      filter: filter,
      attributes: attrs,
      return_result: false
    ) do |entry|
      entry.each do |attribute, values|
        value_str = values.join(',')
        if attrs.include? attribute # skip over dn value returned by the ldap call
          fld_params = schema[attribute]
          user[attribute.to_sym] = validate_field(value_str, fld_params)
        end
      end
    end
    user
  end
end


# Yep
def validate_field (fld_value, fld_params)
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
        # throw an error for unrecognized data type
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

class CsvDestination
  def initialize(file, output_fields)
    @csv = CSV.open(file, 'w')
    @output_fields = output_fields

    @csv << @output_fields
  end

  def write(row)
    verify_row!(row)
    @csv << row.values_at(*@output_fields) #*
  end

  def verify_row!(row)
    missing_fields = @output_fields - [row.keys & @output_fields].flatten

    if missing_fields.size > 0
      raise "Row lacks required field(s) #{missing_fields}\n#{row}"
    end
  end

  def close
    @csv.close
  end
end
