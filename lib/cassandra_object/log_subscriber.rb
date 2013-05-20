module CassandraObject
  class LogSubscriber < ActiveSupport::LogSubscriber
    def add(event)
      return unless logger && logger.debug?
      name = '%s add (%.1fms)' % [event.payload[:column_family], event.duration]

      debug "  #{name}  #{event.payload[:key]}[#{event.payload[:column]}]#{event.payload[:sub_column] ? '[' + event.payload[:sub_column] + ']' : ''} by #{event.payload[:value]}"
    end

    def get(event)
      return unless logger && logger.debug?
      name = '%s get (%.1fms)' % [event.payload[:column_family], event.duration]
      
      debug "  #{name}  #{event.payload[:key]}"
    end

    def get_counter(event)
      return unless logger && logger.debug?
      name = '%s get_counter (%.1fms)' % [event.payload[:column_family], event.duration]

      debug "  #{name}  #{event.payload[:key]}[#{event.payload[:column]}]"
    end

    def multi_get(event)
      return unless logger && logger.debug?
      name = '%s multi_get (%.1fms)' % [event.payload[:column_family], event.duration]

      debug "  #{name}  (#{event.payload[:keys].size}) #{event.payload[:keys].join(" ")}"
    end

    def remove(event)
      return unless logger && logger.debug?
      name = '%s remove (%.1fms)' % [event.payload[:column_family], event.duration]

      message = "  #{name}  #{event.payload[:key]}"
      message << " #{Array(event.payload[:attributes]).inspect}" if event.payload[:attributes]

      debug message
    end

    def truncate(event)
      return unless logger && logger.debug?
      name = '%s truncate (%.1fms)' % [event.payload[:column_family], event.duration]

      debug "  #{name}  #{event.payload[:column_family]}"
    end

    def insert(event)
      return unless logger && logger.debug?
      name = '%s insert (%.1fms)' % [event.payload[:column_family], event.duration]

      debug "  #{name}  #{event.payload[:key]} #{event.payload[:attributes].inspect}"
    end

    def get_range(event)
      return unless logger && logger.debug?
      name = '%s get_range (%.1fms)' % [event.payload[:column_family], event.duration]
      
      debug "  #{name}  (#{event.payload[:count]}) '#{event.payload[:start]}' => '#{event.payload[:finish]}'"
    end

    def get_slice(event)
      return unless logger && logger.debug?
      name = '%s get_slice (%.1fms)' % [event.payload[:column_family], event.duration]
      
      debug "  #{name}  #{event.payload[:key]} '#{event.payload[:start]}' => '#{event.payload[:finish]}'"
    end
  end
end
CassandraObject::LogSubscriber.attach_to :cassandra_object
