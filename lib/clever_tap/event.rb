class CleverTap
  class MissingEventNameError < RuntimeError
    def message
      "Couldn't find `name:` with value in Event#new(options)"
    end
  end

  class Event < Entity
    DATA_STRING = 'evtData'.freeze
    EVENT_NAME_STRING = 'evtName'.freeze
    TYPE_VALUE_STRING = 'event'.freeze
    UPLOAD_LIMIT = 1000
    FETCH_URI = 'events.json'.freeze

    attr_accessor *%i(profile props session_props timestamp)

    def initialize(**args)
      super(**args)
      @name = args[:name]
    end

    def to_h
      super.merge(put_event_name_pair)
    end

    def self.parse(record)
      self.new.tap do |e|
        e.profile = record['profile']
        e.props = record['event_props']
        e.session_props = record['session_props']
        e.timestamp = parse_ts(record['ts'])
      end
    end

    private

    def put_event_name_pair
      raise MissingEventNameError if @name.nil?
      { EVENT_NAME_STRING => @name }
    end

    def self.parse_ts(ts)
      return unless ts
      DateTime.strptime(ts.to_s, '%Y%m%d%H%M%S')
    end
  end
end
