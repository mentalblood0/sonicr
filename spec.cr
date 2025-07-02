require "spec"

require "./src/sonicr"

describe Sonicr do
  describe "Client" do
    Log.setup(:debug)

    collection = "collection"

    client = Sonicr::Client.new "::1", 1491, "SecretPassword"

    Spec.before_each do
      client.start "ingest" { |ingest| ingest.flush collection }
    end

    it "PING" do
      client.start "ingest" { |i| i.ping }
      client.start "search" { |s| s.ping }
    end
    it "HELP" do
      client.start "search" { |s| s.help }
    end
    it "is empty" do
      client.start "search" { |s| (s.wait s.query(collection, "user1", "test")).should eq [] of String }
      client.start "search" { |s| (s.wait s.suggest(collection, "user1", "test")).should eq [] of String }
    end
    it "SUGGEST" do
      bucket = Random::DEFAULT.hex 16
      uid = Random::DEFAULT.hex 16
      client.start "ingest" { |i| i.push(collection, bucket, uid, "RESULT commands(QUERY, SUGGEST, LIST, PING, HELP, QUIT)") }
      client.start "control" { |c| c.trigger "consolidate" }
      client.start "search" { |s| (s.wait s.suggest(collection, bucket, "comm")).should eq ["commands"] }
      client.start "search" { |s| (s.wait s.suggest(collection, bucket, "Q")).should eq ["query", "quit"] }
      client.start "ingest" { |i| i.count(collection, bucket, uid).should eq 8 }
    end
    it "INFO" do
      client.start "control" { |c| c.info }
    end
    it "QUERY" do
      bucket = Random::DEFAULT.hex 16
      uid = Random::DEFAULT.hex 16
      client.start "ingest" { |i| i.push(collection, bucket, uid, "The quick brown fox jumps over the lazy dog") }
      client.start "search" { |s| (s.wait s.query(collection, bucket, "quick", 1, 0)).should eq [uid] }
    end
    it "LIST" do
      bucket = Random::DEFAULT.hex 16
      uid = Random::DEFAULT.hex 16
      client.start "ingest" do |i|
        i.push collection, bucket, uid, "The quick brown fox jumps over the lazy dog"
        i.push collection, bucket, uid, "brown fox jumps"
        i.push collection, bucket, uid, "lazy dog jumps"
      end
      client.start "control" { |c| c.trigger "consolidate" }
      client.start "search" { |s| (s.wait s.list collection, bucket).sort.should eq ["brown", "dog", "fox", "jumps", "lazy", "quick"] }
    end
    it "FLUSHB" do
      bucket = Random::DEFAULT.hex 16
      uid = Random::DEFAULT.hex 16
      client.start "ingest" { |i| i.push(collection, bucket, uid, "The quick brown fox jumps over the lazy dog") }
      client.start "search" { |s| (s.wait s.query(collection, bucket, "quick", 1, 0)).should eq [uid] }
      client.start "ingest" { |i| i.flush(collection, bucket).should eq 1 }
      client.start "search" { |s| (s.wait s.query(collection, bucket, "quick", 1, 0)).should eq [] of String }
    end
    it "FLUSHC" do
      bucket = Random::DEFAULT.hex 16
      uid = Random::DEFAULT.hex 16
      client.start "ingest" { |i| i.push(collection, bucket, uid, "The quick brown fox jumps over the lazy dog") }
      client.start "search" { |s| (s.wait s.query(collection, bucket, "quick", 1, 0)).should eq [uid] }
      client.start "ingest" { |i| i.flush(collection).should eq 1 }
      client.start "search" { |s| (s.wait s.query(collection, bucket, "quick", 1, 0)).should eq [] of String }
    end
    it "FLUSHO" do
      bucket = Random::DEFAULT.hex 16
      uid = Random::DEFAULT.hex 16
      client.start "ingest" { |i| i.push(collection, bucket, uid, "The quick brown fox jumps over the lazy dog") }
      client.start "search" { |s| (s.wait s.query(collection, bucket, "quick", 1, 0)).should eq [uid] }
      client.start "ingest" { |i| i.flush(collection, bucket, uid).should eq 6 }
      client.start "search" { |s| (s.wait s.query(collection, bucket, "quick", 1, 0)).should eq [] of String }
    end
    it "POP" do
      bucket = Random::DEFAULT.hex 16
      uid = Random::DEFAULT.hex 16
      client.start "ingest" do |i|
        i.push(collection, bucket, uid, "The quick brown fox jumps over the lazy dog")
        i.pop(collection, bucket, uid, "quick").should eq 1
        i.count(collection, bucket, uid).should eq 5
      end
      client.start "search" { |s| (s.wait s.query(collection, bucket, "quick")).should eq [] of String }
    end
    it "INGEST" do
      bucket = Random::DEFAULT.hex 16
      uid = Random::DEFAULT.hex 16
      client.start "ingest" { |i| i.push(collection, bucket, uid, "żółć") }
      client.start "search" { |s| (s.wait s.query(collection, bucket, "żółć")).should eq [uid] }
      long_string = (Array.new 1000 { |a| Random::DEFAULT.hex 20 }).join(' ')
      client.start "ingest" { |i| expect_raises Sonicr::Exception do
        i.push(collection, bucket, uid, long_string)
      end }
    end
    it "LIMIT, OFFSET" do
      bucket = Random::DEFAULT.hex 16
      uid = Random::DEFAULT.hex 16
      uid2 = Random::DEFAULT.hex 16
      client.start "ingest" do |i|
        i.push(collection, bucket, uid, "The quick brown fox jumps over the lazy dog")
        i.push(collection, bucket, uid2, "The quick brown fox jumps over the lazy dog complete")
      end
      client.start "search" do |s|
        (s.wait s.query(collection, bucket, "fox")).should eq [uid2, uid]
        (s.wait s.query(collection, bucket, "fox", limit: 1)).should eq [uid2]
        (s.wait s.query(collection, bucket, "fox", limit: 1, offset: 1)).should eq [uid]
      end
    end
    it "raises exception when mix commands" do
      client.start "search" { |i| expect_raises Sonicr::Exception do
        i.push(collection, "bucket", "uid", "text")
      end }
    end
  end
end
