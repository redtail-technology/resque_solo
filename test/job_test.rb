# frozen_string_literal: true

require "test_helper"

class JobTest < MiniTest::Spec
  before do
    Resque.redis.redis.flushdb
  end

  it "enqueue identical jobs once" do
    Resque.enqueue FakeUniqueJob, "x"
    Resque.enqueue FakeUniqueJob, "x"
    assert_equal 1, Resque.size(:unique)
  end

  it "with parallel calls still enqueues identical jobs once" do
    # Ruby threads do not run in parallel. So, to simulate parallelism we stub the enqueue_job
    # method and add a sleep, which releases the global interpreter lock and gives other threads a
    # chance to execute.
    enqueue_job = Resque::Job.method(:enqueue_job)
    enqueue_job_stub = lambda do |*args|
      sleep 0.01
      enqueue_job.call(*args)
    end

    Resque::Job.stub :enqueue_job, enqueue_job_stub do
      threads = 4.times.map do
        Thread.new { Resque.enqueue FakeUniqueJob, "x" }
      end
      threads.each(&:join)

      assert_equal 1, Resque.size(:unique)
    end
  end

  it "allow the same jobs to be executed one after the other" do
    Resque.enqueue FakeUniqueJob, "foo"
    Resque.enqueue FakeUniqueJob, "foo"
    assert_equal 1, Resque.size(:unique)
    Resque.reserve(:unique)
    assert_equal 0, Resque.size(:unique)
    Resque.enqueue FakeUniqueJob, "foo"
    Resque.enqueue FakeUniqueJob, "foo"
    assert_equal 1, Resque.size(:unique)
  end

  it "consider equivalent hashes regardless of key order" do
    Resque.enqueue FakeUniqueJob, bar: 1, foo: 2
    Resque.enqueue FakeUniqueJob, foo: 2, bar: 1
    assert_equal 1, Resque.size(:unique)
  end

  it "treat string and symbol keys equally" do
    Resque.enqueue FakeUniqueJob, bar: 1, foo: 1
    Resque.enqueue FakeUniqueJob, bar: 1, "foo" => 1
    assert_equal 1, Resque.size(:unique)
  end

  it "mark jobs as unqueued, when Job.destroy is killing them" do
    Resque.enqueue FakeUniqueJob, "foo"
    Resque.enqueue FakeUniqueJob, "foo"
    assert_equal 1, Resque.size(:unique)
    Resque::Job.destroy(:unique, FakeUniqueJob)
    assert_equal 0, Resque.size(:unique)
    Resque.enqueue FakeUniqueJob, "foo"
    Resque.enqueue FakeUniqueJob, "foo"
    assert_equal 1, Resque.size(:unique)
  end

  it "mark jobs as unqueued when they raise an exception" do
    2.times { Resque.enqueue(FailingUniqueJob, "foo") }
    assert_equal 1, Resque.size(:unique)
    worker = Resque::Worker.new(:unique)
    worker.work 0
    assert_equal 0, Resque.size(:unique)
    2.times { Resque.enqueue(FailingUniqueJob, "foo") }
    assert_equal 1, Resque.size(:unique)
  end

  it "report if a unique job is enqueued" do
    Resque.enqueue FakeUniqueJob, "foo"
    assert Resque.enqueued?(FakeUniqueJob, "foo")
    refute Resque.enqueued?(FakeUniqueJob, "bar")
  end

  it "report if a unique job is enqueued in another queue" do
    default_queue = FakeUniqueJob.instance_variable_get(:@queue)
    FakeUniqueJob.instance_variable_set(:@queue, :other)
    Resque.enqueue FakeUniqueJob, "foo"
    assert Resque.enqueued_in?(:other, FakeUniqueJob, "foo")
    FakeUniqueJob.instance_variable_set(:@queue, default_queue)
    refute Resque.enqueued?(FakeUniqueJob, "foo")
  end

  it "cleanup when a queue is destroyed" do
    Resque.enqueue FakeUniqueJob, "foo"
    Resque.enqueue FailingUniqueJob, "foo"
    Resque.remove_queue(:unique)
    Resque.enqueue(FakeUniqueJob, "foo")
    assert_equal 1, Resque.size(:unique)
  end

  it "honor ttl in the redis key" do
    Resque.enqueue UniqueJobWithTtl
    assert Resque.enqueued?(UniqueJobWithTtl)
    keys = Resque.redis.keys "solo:queue:unique_with_ttl:job:*"
    assert_equal 1, keys.length
    assert_in_delta UniqueJobWithTtl.ttl, Resque.redis.ttl(keys.first), 2
  end

  it "prevents duplicates until completion with release_lock_after_completion" do
    Resque.enqueue UniqueJobWithLock, "foo"
    Resque.enqueue UniqueJobWithLock, "foo"
    assert_equal 1, Resque.size(:unique_with_lock)
    Resque.reserve(:unique_with_lock)
    assert_equal 0, Resque.size(:unique_with_lock)
    Resque.enqueue UniqueJobWithLock, "foo"
    assert_equal 0, Resque.size(:unique_with_lock)
    UniqueJobWithLock.after_perform_release_lock("foo")
    Resque.enqueue UniqueJobWithLock, "foo"
    assert_equal 1, Resque.size(:unique_with_lock)
  end

  it "honor release_lock_after_completion in the redis key" do
    Resque.enqueue UniqueJobWithLock
    Resque.reserve(:unique_with_lock)
    keys = Resque.redis.keys "solo:queue:unique_with_lock:job:*"
    assert_equal 1, keys.length
    assert_equal UniqueJobWithLock.ttl, Resque.redis.ttl(keys.first), 2
  end

  it "given args and string metadata returns args with metadata" do
    Resque.enqueue FakeUniqueJob, "foo", "metadata" => { "foo" => "bar" }
    job = Resque.reserve(:unique)

    payload = {
      "class" => "FakeUniqueJob",
      "args" => ["foo", { "metadata" => { "foo" => "bar" } }]
    }

    assert_equal payload, job.payload
  end

  it "given args and symbolized metadata returns args with metadata" do
    Resque.enqueue FakeUniqueJob, "foo", metadata: { foo: "bar" }
    job = Resque.reserve(:unique)

    payload = {
      "class" => "FakeUniqueJob",
      "args" => ["foo", { "metadata" => { "foo" => "bar" } }]
    }

    assert_equal payload, job.payload
  end

  it "given args and no metadata returns args without metadata" do
    Resque.enqueue FakeUniqueJob, "foo"
    job = Resque.reserve(:unique)

    payload = {
      "class" => "FakeUniqueJob",
      "args" => ["foo"]
    }

    assert_equal payload, job.payload
  end

  it "given hash args returns args with metadata" do
    Resque.enqueue FakeUniqueJob, foo: "foo", bar: "bar", metadata: { foo: "foo" }
    job = Resque.reserve(:unique)

    payload = {
      "class" => "FakeUniqueJob",
      "args" => [{ "foo" => "foo", "bar" => "bar", "metadata" => { "foo" => "foo" }}]
    }

    assert_equal payload, job.payload
  end

  it "given normal job with primitive args returns unaltered args" do
    Resque.enqueue FakeJob, "foo"
    job = Resque.reserve(:normal)

    payload = {
      "class" => "FakeJob",
      "args" => ["foo"]
    }

    assert_equal payload, job.payload
  end

  it "given normal job with named arguments returns unaltered args" do
    Resque.enqueue FakeJob, "foo", "bar", foo: "foo", bar: "bar", "baz" => "baz"
    job = Resque.reserve(:normal)

    payload = {
      "class" => "FakeJob",
      "args" => ["foo", "bar", { "foo" => "foo", "bar" => "bar", "baz" => "baz" }]
    }

    assert_equal payload, job.payload
  end

  it "given normal job with named arguments and metadata returns unaltered args" do
    Resque.enqueue FakeJob, foo: "foo", metadata: { bar: "bar" }
    job = Resque.reserve(:normal)

    payload = {
      "class" => "FakeJob",
      "args" => [{ "foo" => "foo", "metadata" => { "bar" => "bar" }}]
    }

    assert_equal payload, job.payload
  end
end
