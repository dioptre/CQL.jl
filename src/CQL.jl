module CQL

####################################################################
# CQLConnection
####################################################################

type CQLConnection
  socket  :: Base.TCPSocket
  buffer  :: IOBuffer
  msg_id  :: UInt8
  replies :: Dict
  pending :: Int
end

####################################################################
# Helpers
####################################################################

function ticks()
	return round(Int, time()*1000)
end

function die(;pid=getpid())
	ccall( (:kill, "libc"), Int32, (Int32,Int32), pid, 9) #Need this to force Julia to not hang
end

####################################################################
# Connect & Disconnect
####################################################################

function connect(server::AbstractString = "localhost", port::Int = 9042)
  con = CQLConnection(Base.connect(server, port),
                      IOBuffer(), 1, Dict(), 0);
  sendMessage(con, 0x01, Dict("CQL_VERSION" => "3.0.0"));
  version, id, opcode, len = readServerMessage(con.socket);
  con.pending = 0;
  @async handleServerMessages(con);
  @assert version == 0x82 || version == 0x84 || version == 0x83
  @assert opcode  == 0x02

  con
end

function disconnect(con::CQLConnection)
  while 0 < con.pending
    yield();
  end
  close(con.socket);
  con.socket  = Base.TCPSocket();
  con.buffer  = IOBuffer();
  con.msg_id  = 1;
  con.replies = Dict();
  con.pending = 0;
  con
end

####################################################################
# Handle Server Messages
####################################################################

function readServerMessage(socket::Base.TCPSocket)
  version = read(socket, UInt8);
  flags   = read(socket, UInt8);
  id      = read(socket, UInt16);
  opcode  = read(socket, UInt8);
  len     = ntoh(read(socket, UInt32));
  (version, id, opcode, len)
end

function handleServerMessage(con::CQLConnection)
  version, id, opcode, len = readServerMessage(con.socket);
  if id > 0 then
    put!(pop!(con.replies, id),
         (opcode, readbytes(con.socket, len)));
  elseif opcode == 0x00 then
    kind   = ntoh(read(con.socket, Int32));
    strlen = ntoh(read(con.socket, Int16));
    print("ERROR (HANDLE) [$kind] : "*
            bytestring(readbytes(con.socket, strlen)));
    throw(InterruptException())
  else
    for _ in 1:len
      read(con.socket, UInt8);
    end
  end
  con.pending -= 1;
  nothing
end

function handleServerMessages(con::CQLConnection)

  while !eof(con.socket)
    try
      handleServerMessage(con);
    catch err

      if !isa(err, EOFError) then
	close(con.socket)
	yield()
	#die() #bit harsh forcing thread to die - moved outside, might work with spawnat if julia hangs again
	Base.throwto(current_task(), InterruptException())
      else
	nothing
      end

    end
  end
  nothing
end


####################################################################
# Timestamp
####################################################################

type Timestamp
  milliseconds::Int64
end

const timeOffset = time(Libc.strptime("%F %T %z %Z",
                                 "1970-01-01 00:00:00 +0000 UTC"));

function Timestamp(str::AbstractString)
  Timestamp(1000 * int64(time(Libc.strptime("%F %T", str)) + timeOffset))
end

function Base.print(io::IO, t::Timestamp)
  print(io, "ts\"");
  print(io, strftime("%F %T", div(t.milliseconds, 1000)
                              - timeOffset));
  print(io, "\"");
  nothing
end

show(io::IO, t::Timestamp) = print(io, t);

macro ts_str(p)
  Timestamp(p)
end

####################################################################
# Decoding
####################################################################

function decodeString(s::IO)
  strlen = ntoh(read(s, Int16));
  bytestring(readbytes(s, strlen));
end

function decodeResultSubColumn(s::IO, typ)
  nrOfBytes = ntoh(read(s, Int32));
  decodeValue(s, nrOfBytes, (typ, nothing, nothing))
end

function decodeList(s::IO, typ)
  nrOfElements = ntoh(read(s, UInt32));
  ar = Array(Any, nrOfElements);
  for ix in 1:nrOfElements
    ar[ix] = decodeResultSubColumn(s, typ);
  end
  ar
end

function decodeMap(s::IO, val_type)
  Set(decodeList(s, val_type))
end

function decodeDict(s::IO, key_type, val_type)
  d = Dict();
  nrOfElements = Int(ntoh(read(s, UInt32)));
  for i in 1:nrOfElements
    key = decodeResultSubColumn(s, key_type);
    val = decodeResultSubColumn(s, val_type);
    d[key] = val;
  end
  d
end

function decodeValue(s::IO, nrOfBytes::Integer, types)
  type_kind, val_type, key_type = types;
  nrOfBytes < 0     ? nothing :
  type_kind == 0x02 ? ntoh(read(s, UInt64)) :
  type_kind == 0x09 ? Int(ntoh(read(s, UInt32))) :
  type_kind == 0x0B ? (Timestamp(ntoh(read(s, UInt64)))) :
  type_kind == 0x0C ? (Base.Random.UUID(ntoh(read(s, UInt128)))) :
  type_kind == 0x0D ? bytestring(readbytes(s, nrOfBytes)) :
  type_kind == 0x20 ? decodeList(s, val_type) :
  type_kind == 0x21 ? decodeDict(s, key_type, val_type) :
  type_kind == 0x22 ? decodeMap(s, val_type) :
            ("*NYI*", type_kind, readbytes(s, nrOfBytes))
end

function decodeResultRowTypes(s::IOBuffer)
  flags   = ntoh(read(s, UInt32));
  colcnt  = ntoh(read(s, UInt32));
  gl_spec = (flags & 0x0001) != 0;

  if gl_spec then
    gl_ksname = decodeString(s);
    gl_tablename = decodeString(s);
  end
  if (flags & 0x0002) != 0 then
    println("d >> ");
  end
  if (flags & 0x0004) != 0 then
    println("e >> ");
  end

  types = Array(Tuple{Int16,Any,Any}, colcnt);
  for col in 1:colcnt
    ksname     = gl_spec ? gl_ksname : decodeString(s);
    tablename  = gl_spec ? gl_tablename : decodeString(s);
    name       = decodeString(s);
    kind       = ntoh(read(s, UInt16));
    key_type   = kind == 0x21 ?  ntoh(read(s, UInt16)) : nothing;
    sub_type   = kind in UInt8[0x20, 0x21, 0x22] ? ntoh(read(s, UInt16)) : nothing;
    types[col] = (kind, sub_type, key_type);
  end
  types
end

function decodeResultColumn(s::IO, typ)
  nrOfBytes = ntoh(read(s, Int32));
  decodeValue(s, nrOfBytes, typ)
end

function decodeResultRows(s::IOBuffer)
  types = decodeResultRowTypes(s);
  nrOfColumns = length(types);
  nrOfRows = ntoh(read(s, UInt32));
  rows = Array(Array{Any}, nrOfRows)
  for rix = 1:nrOfRows
    rows[rix] = Array(Any, nrOfColumns);
    for cix in 1:nrOfColumns
      rows[rix][cix] = decodeResultColumn(s, types[cix])
    end
  end
  rows
end

function decodeResultMessage(buffer::Array{UInt8})
  s = IOBuffer(buffer)
  kind = ntoh(read(s, UInt32))
  return kind == 0x01 ? ("void") :
  kind == 0x02 ? decodeResultRows(s) :
  kind == 0x03 ? ("set keyspace", decodeString(s)) :
  kind == 0x04 ? ("prepared") :
  kind == 0x05 ? ("schema change", decodeString(s), decodeString(s), decodeString(s)) :
    ("???");
end

function decodeErrorMessage(buffer::Array{UInt8})
  s = IOBuffer(buffer);
  kind = ntoh(read(s, UInt32));
  errmsg = decodeString(s);
  println("ERROR (DECODE) [$kind]: ", errmsg);
  ("ERROR", kind, errmsg)
end

function decodeMessage(opcode::UInt8, buffer::Array{UInt8})
  opcode == 0x08 ? decodeResultMessage(buffer) :
  opcode == 0x00 ? decodeErrorMessage(buffer) :
         (opcode, buffer);
end

####################################################################
# Encoding
####################################################################

function cql_encode_string(buf::IOBuffer, str::AbstractString)
  encStr = bytestring(isvalid(UTF8String, str) ? str : utf8(str));
  write(buf, hton(UInt16(sizeof(encStr))));
  write(buf, encStr);
  nothing
end

function cql_encode_long_string(buf::IOBuffer, str::AbstractString)
  encStr = bytestring(isvalid(UTF8String, str) ? str : utf8(str));
  write(buf, hton(UInt32(sizeof(encStr))));
  write(buf, encStr);
  nothing
end

function cql_encode(buf::IOBuffer, dict::Dict)
  write(buf, hton(UInt16(length(dict))));
  for (k,v) in dict
    cql_encode_string(buf, k);
    cql_encode_string(buf, v);
  end
  nothing
end

function cql_encode(buf::IOBuffer, query::AbstractString)
  cql_encode_long_string(buf, query);
  write(buf, 0x00);
  write(buf, 0x04);
  write(buf, 0x00);
  nothing
end

####################################################################
# Sending Message to the server
####################################################################

function sendMessageBody(con::CQLConnection, msg)
  buf = con.buffer;
  truncate(buf, 0);
  cql_encode(buf, msg);
  write(con.socket, hton(UInt32(buf.size)));
  write(con.socket, takebuf_array(buf));
end

function sendMessage(con::CQLConnection, kind::UInt8, msg,
                     id::UInt16 = 0x0000)
  con.pending += 1;
  write(con.socket, 0x03);
  write(con.socket, 0x00);
  write(con.socket, id);
  write(con.socket, kind);
  sendMessageBody(con, msg);

  flush(con.socket);
  yield();
  nothing
end

function nextReplyID(con::CQLConnection)
  id::UInt8 = con.msg_id;
  con.msg_id = 1 + (id + 1) % 99;
  while haskey(con.replies, id)
    yield();
  end
  reply = RemoteRef();
  con.replies[id] = reply;
  (id, reply)
end

####################################################################
# Queries
####################################################################

function query(con::CQLConnection, msg::AbstractString)
  sync(con);
  getResult(asyncQuery(con, msg))
end

function command(con::CQLConnection, msg::AbstractString)
  sync(con);
  asyncCommand(con, msg)
  sync(con);
  nothing
end

function asyncQuery(con::CQLConnection, msg::AbstractString)
  id::UInt16, reply = nextReplyID(con);
  sendMessage(con, 0x07, msg, id);
  reply
end

function asyncCommand(con::CQLConnection, msg::AbstractString)
  sendMessage(con, 0x07, msg);
  nothing
end

function getResult(reply::RemoteRef)
  decodeMessage(take!(reply) ...)
end

function sync(con::CQLConnection)
  while 0 < con.pending
    yield();
  end
end


####################################################################
# Helpers
####################################################################

cleanString(str) = replace(str,"'","\\'")
originalString(str) = replace(str,"\\'","'")

####################################################################

end
