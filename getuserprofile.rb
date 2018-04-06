# Retrieve JHED user profile field values based on
# JHED ID
require 'json'
require 'net/ldap'
require 'yaml'

require_relative 'utilities'

class GetUserProfile
  include FieldPrep

  attr_accessor :jhed, :profile_fields
  attr_accessor :retrieved_profile

  def initialize
    ldap_config = YAML.load(File.open('.config/ldap_connection.yaml'))
    @ldap = Net::LDAP.new ldap_config['active_env']
    @tree = ldap_config['treebase']
  end

  def ldap_lookup (id_value)
    # puts "LDAP2 = #{@ldap}"
    # puts "ID = #{id_value}"
   filter = Net::LDAP::Filter.eq('uid', id_value)
    user = {}
    attrs = YAML.load(File.open('jhed_profile_schema.yaml')).keys
    @ldap.search(
      base: @tree,
      filter: filter,
      attributes: attrs,
      return_result: false
    ) do |entry|
  #    puts "entry #{entry}"
      entry.each do |attribute, values|
      #  puts "Values #{values}"
        user[attribute.to_sym] = '' if attribute
        user[attribute.to_sym] += values.join(',') if values
      end
    end
    user
  end
end
