# frozen_string_literal: true

require "digest/md5"

module Resque
  module Plugins
    module UniqueJob
      def self.included(base)
        base.extend ClassMethods
      end

      module ClassMethods
        # Payload is what Resque stored for this job along with the job's class name:
        # a hash containing :class and :args
        def redis_key(payload)
          payload = Resque.decode(Resque.encode(payload))
          job  = payload['class']
          args = payload['args']
          args.map! do |arg|
            arg.is_a?(Hash) ? arg.sort : arg
          end

          Digest::MD5.hexdigest Resque.encode(class: job, args: args)
        end

        # The default ttl of a locking key is -1 (forever).
        # To expire the lock after a certain amount of time, set a ttl (in seconds).
        # For example:
        #
        # class FooJob
        #   include Resque::Plugins::UniqueJob
        #   @ttl = 40
        # end
        def ttl
          if release_lock_after_completion && @ttl.nil?
            # set a default ttl of 1 day for jobs with release_lock_after_completion in case
            # the job crashes and after_perform_release_lock is never called
            @ttl ||= 86400
          else
            @ttl ||= -1
          end
        end

        # The default ttl of a persisting key is 0, i.e. immediately deleted.
        # Set release_lock_after_completion to block the execution
        # of another job until the current one completes.
        # For example:
        #
        # class FooJob
        #   include Resque::Plugins::UniqueJob
        #   @release_lock_after_completion = true
        # end
        def release_lock_after_completion
          @release_lock_after_completion ||= false
        end

        def after_perform_release_lock(*args)
          if release_lock_after_completion
            key = ResqueSolo::Queue.unique_key(@queue, { class: name, args: args })
            Resque.redis.del(key)
          end
        end
      end
    end
  end
end
