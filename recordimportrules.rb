# This class reads the yaml_file.yaml file
class RecordImportRules
  require 'yaml'
  attr_reader :yaml_file

  def initialize
    @yaml_file = YAML.load(File.open('record_import_rules.yaml'))
  end

  # IMPORT RULES LIST
  def import_rules_list
    @yaml_file.keys
  end

  def import_params(imp)
    @yaml_file[imp]
  end

  def target_table_name(imp)
    @yaml_file[imp][:target_table][:table_name]
  end

  def source_table_name(imp)
    @yaml_file[imp][:source_table][:table_name]
  end

  def target_connection(imp)
    @yaml_file[imp][:target_table][:db_connection]
  end

  def source_connection(imp)
    @yaml_file[imp][:source_table][:db_connection]
  end

  def target_fields_list(imp)
    @yaml_file[imp][:target_table][:table_fields].keys
  end

  def source_fields_list(imp)
    @yaml_file[imp][:source_table][:table_fields].keys
  end
end
