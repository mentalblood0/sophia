@[Link(ldflags: "#{__DIR__}/sophia.o")]
lib LibSophia
  fun env = sp_env : Void*
  fun document = sp_document(object : Void*) : Void*
  fun setstring = sp_setstring(object : Void*, path : UInt8*, ptr : Void*, size : Int32) : Int32
  fun setint = sp_setint(object : Void*, path : UInt8*, value : Int64) : Int64
  fun getobject = sp_getobject(object : Void*, path : UInt8*) : Void*
  fun getstring = sp_getstring(object : Void*, path : UInt8*, size : Int32*) : Void*
  fun getint = sp_getint(object : Void*, path : UInt8*) : Int64
  fun open = sp_open(object : Void*) : Int32
  fun destroy = sp_destroy(object : Void*) : Int32
  fun set = sp_set(object : Void*, document : Void*) : Int32
  fun upsert = sp_upsert(object : Void*, document : Void*) : Int32
  fun delete = sp_delete(object : Void*, document : Void*) : Int32
  fun get = sp_get(object : Void*, document : Void*) : Void*
  fun cursor = sp_cursor(env : Void*) : Void*
  fun begin = sp_begin(env : Void*) : Void*
  fun prepare = sp_prepare(transaction : Void*) : Int32
  fun commit = sp_commit(transaction : Void*) : Int32
end
