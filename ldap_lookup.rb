require 'awesome_print'
require 'net/ldap'
require 'json'
require 'yaml'

class LdapLookup
  # create the ldap connection
  def initialize(env)

    ldap_connection_params = YAML.load(File.open('.config/ldap_connection.yaml'))[env]
    @ldap = Net::LDAP.new ldap_connection_params
  end

  def ldap_lookup (id_value)
    puts "LDAP2 = #{@ldap}"
    puts "ID = #{id_value}"
    filter = Net::LDAP::Filter.eq('uid', id_value)
    treebase = 'ou=people,dc=win,dc=ad,dc=jhu,dc=edu'
    user = {}
    attrs = %w[dn uid givenname sn mail telephonenumber edupersonaffiliation
               edupersonorgunitdn jhejcardbarcode ou]
    @ldap.search(
      base: treebase,
      filter: filter,
      attributes: attrs,
      return_result: false
    ) do |entry|
      puts "entry #{entry}"
      entry.each do |attribute, values|
        puts "Values #{values}"
        user[attribute.to_sym] = '' if attribute
        user[attribute.to_sym] += values.join(',') if values
      end
    end
    user
  end
end
