class CleverTap
  class Profile < Entity
    DATA_STRING = 'profileData'.freeze
    TYPE_VALUE_STRING = 'profile'.freeze
    UPLOAD_LIMIT = 100
    FETCH_URI = 'profiles.json'.freeze

    attr_accessor *%i(identity email events platforms profile_data)

    def self.parse(record)
      self.new.tap do |p|
        p.identity = record['identity']
        p.email = record['email']
        p.profile_data = record['profileData'] || {}
        p.profile_data['name'] = record['name']
        p.events = record['events'] || {}
        p.platforms = record['platformInfo'] || []
      end
    end
  end
end
