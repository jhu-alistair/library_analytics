require 'awesome_print'
require 'net/ldap'
require 'json'
require 'yaml'

class LdapLookup
  # create the ldap connection
  def initialize(env)
  ldap_connection_params = YAML.load(File.open('.config/ldap_connection.yaml'))[env]
    @ldap = Net::LDAP.new ldap_connection_params
    @tree = YAML.load(File.open('.config/ldap_connection.yaml'))['treebase']
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
