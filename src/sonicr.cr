require "log"
require "socket"

module Sonicr
  class Exception < Exception
  end

  class Client
    def initialize(@host : String, @port : Int32, @password : String)
      @buffer = 20000
      @events = {} of String => Array(String)
    end

    def start(mode : String, &)
      @socket = TCPSocket.new @host, @port

      expect /^CONNECTED <sonic-server v\d+\.\d+\.\d+>$/
      send "START #{mode} #{@password}"
      @buffer = expect(/^STARTED #{mode} protocol\(1\) buffer\((\d+)\)$/)[1].to_i
      yield self

      send "QUIT"
      expect /^ENDED quit$/
      @events.clear
    end

    def send(s : String)
      raise Exception.new "request do not fit buffer size" unless s.size + 1 < @buffer
      Log.debug { ">> #{s}" }
      @socket.not_nil!.send "#{s}\n"
    end

    def expect(regex : Regex)
      aa = ""
      loop do
        l = @socket.not_nil!.read_line
        Log.debug { "<< #{l}" }

        m = l.match regex
        return m if m = l.match regex

        if em = l.match /^EVENT ((?:QUERY|SUGGEST) [^ ]+) (.+)?$/
          @events[em[1]] = (em[2].split ' ' rescue [] of String)
        else
          raise Exception.new "Expected #{regex} but got \"#{aa}\"" unless m
        end
      end
    end

    def escape(text : String)
      text.sub '"', "\\\""
    end

    def push(collection : String, bucket : String, object : String, text : String, lang : String?)
      query = String.build do |s|
        s << "PUSH #{collection} #{bucket} #{object} \"#{escape text}\""
        s << " LANG(#{lang})" if lang
      end
      send query
      expect /^OK$/
    end

    def pop(collection : String, bucket : String, object : String, text : String)
      send "POP #{collection} #{bucket} #{object} \"#{escape text}\""
      expect(/^RESULT (\d+)$/)[1].to_i
    end

    def count(collection : String, bucket : String? = nil, object : String? = nil)
      raise Exception.new "provide bucket too" if object && !bucket
      send "COUNT #{collection} #{bucket} #{object}"
      expect(/^RESULT (\d+)$/)[1].to_i
    end

    def flush(collection : String, bucket : String? = nil, object : String? = nil)
      raise Exception.new "provide bucket too" if object && !bucket
      if object
        send "FLUSHO #{collection} #{bucket} #{object}"
      elsif bucket
        send "FLUSHB #{collection} #{bucket}"
      else
        send "FLUSHC #{collection}"
      end
      expect(/^RESULT (\d+)$/)[1].to_i
    end

    def query(collection : String, bucket : String, text : String, limit : Int32? = nil, offset : Int32? = nil, lang : String? = nil)
      query = String.build do |s|
        s << "QUERY #{collection} #{bucket} \"#{escape text}\""
        s << " LIMIT(#{limit})" if limit
        s << " OFFSET(#{offset})" if offset
        s << " LANG(#{lang})" if lang
      end
      send query
      "QUERY " + expect(/^PENDING ([^\n]+)$/)[1]
    end

    def suggest(collection : String, bucket : String, text : String, limit : Int32? = nil)
      escaped_text = text.sub '"', "\\\""
      query = String.build do |s|
        s << "SUGGEST #{collection} #{bucket} \"#{escape text}\""
        s << " LIMIT(#{limit})" if limit
      end
      send query
      "SUGGEST " + expect(/^PENDING ([^\n]+)$/)[1]
    end

    def wait(event : String)
      unless result = @events.delete event
        expect(/EVENT #{event} (.+)?/)[1].split ' ' rescue [] of String
      else
        return result
      end
    end

    def list(collection : String, bucket : String, limit : Int32? = nil, offset : Int32? = nil)
      query = String.build do |s|
        s << "LIST #{collection} #{bucket}"
        s << " LIMIT(#{limit})" if limit
        s << " OFFSET(#{offset})" if offset
      end
      send query
      "LIST " + expect(/^PENDING ([^\n]+)$/)[1]
    end

    def ping
      send "PING"
      expect /^PONG$/
    end

    def help(manual : String = "commands")
      send "HELP #{manual}"
      expect(/^RESULT #{manual}\(([A-Z, ]+)\)$/)[1].split(", ")
    end

    def trigger(action : String, data : String? = nil)
      send "TRIGGER #{action} #{data}"
      expect /^OK$/
    end

    def info
      send "INFO"
      result = {} of String => Int32
      expect(/^RESULT (.*)$/)[1].split(' ').each do |p|
        m = p.match! /(\w+)\((\d+)\)/
        result[m[1]] = m[2].to_i32
      end
      result
    end
  end
end
