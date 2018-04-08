# Retrieve JHED user profile field values based on
# JHED ID
require 'awesome_print'
require 'json'
require 'net/ldap'
require 'yaml'

require_relative 'utilities'

class GetUserProfile
  include FieldPrep

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
