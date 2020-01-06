# frozen_string_literal: true

module ResqueSolo
  class Queue
    class << self
      def queued?(queue, item)
        return false unless is_unique?(item)
        redis.get(unique_key(queue, item)) != nil
      end

      def mark_queued(queue, item, metadata)
        return unless is_unique?(item)
        key = unique_key(queue, item)
        redis.set(key, metadata.nil? ? 1 : metadata.to_json)
        ttl = item_ttl(item)
        redis.expire(key, ttl) if ttl >= 0
      end

      def mark_unqueued(queue, job)
        item = job.is_a?(Resque::Job) ? job.payload : job
        return unless is_unique?(item)
        release_after = release_lock_after_completion(item)
        metadata = redis.get(unique_key(queue, item))
        redis.del(unique_key(queue, item)) unless release_after
        metadata == '1' ? true : metadata
      end

      def unique_key(queue, item)
        "solo:queue:#{queue}:job:#{const_for(item).redis_key(item)}"
      end

      def is_unique?(item)
        const_for(item).included_modules.include?(::Resque::Plugins::UniqueJob)
      rescue NameError
        false
      end

      def item_ttl(item)
        const_for(item).ttl
      rescue NameError
        -1
      end

      def release_lock_after_completion(item)
        const_for(item).release_lock_after_completion
      rescue NameError
        false
      end

      def destroy(queue, klass, *args)
        klass = klass.to_s
        redis_queue = "queue:#{queue}"

        redis.lrange(redis_queue, 0, -1).each do |string|
          json = Resque.decode(string)
          next unless json["class"] == klass
          next if args.any? && json["args"] != args
          ResqueSolo::Queue.mark_unqueued(queue, json)
        end
      end

      def cleanup(queue)
        cursor = "0"
        loop do
          cursor, keys = redis.scan(cursor, match: "solo:queue:#{queue}:job:*")
          redis.del(*keys) if keys.any?
          break if cursor.to_i.zero?
        end
      end

      private

      def redis
        Resque.redis
      end

      def item_class(item)
        item[:class] || item["class"]
      end

      def const_for(item)
        Resque::Job.new(nil, nil).constantize item_class(item)
      end
    end
  end
end
