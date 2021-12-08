// -*- c++ -*-
#ifndef RPCXX_H
#define RPCXX_H

#include <cstdlib>
#include "rpc.h"

namespace rpc {

// Protocol is used for encode and decode a type to/from the network.
//
// You may use network byte order, but it's optional. We won't test your code
// on two different architectures.

// TASK1: add more specializations to Protocol template class to support more
// types.
template <typename T> struct Protocol {
  
  /* out_bytes: Write data into this buffer. It's size is equal to *out_len
   *   out_len: Initially, *out_len tells you the size of the buffer out_bytes.
   *            However, it is also used as a return value, so you must set *out_len
   *            to the number of bytes you wrote to the buffer.
   *         x: the data you want to write to buffer
   */   	
  static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const T &x) {
    return false;
  }
  
  /* in_bytes: Read data from this buffer. It's size is equal to *in_len
   *   in_len: Initially, *in_len tells you the size of the buffer in_bytes.
   *           However, it is also used as a return value, so you must set *in_len
   *           to the number of bytes you consume from the buffer.
   *        x: the data you want to read from the buffer
   */   
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, T &x) {
    return false;
  }
};

// ---------- old code from lab ------------
/*
template <> struct Protocol<int> {
  static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const int &x) {
	// check if buffer is big enough to fit the data, if not, return false
    if (*out_len < sizeof(int)) return false; 
	
	// do a memory copy of the data into the buffer, TYPE_SIZE is the size of the data
    memcpy(out_bytes, &x, sizeof(int));
	
	// since we wrote TYPE_SIZE number of bytes to the buffer, we set *out_len to TYPE_SIZE
    *out_len = sizeof(int);

    return true;
  }
  
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, int &x) {
	// check if buffer is big enough to read in x, if not, return false
    if (*in_len < sizeof(int)) return false;
	
	// do a memory copy from the buffer into the data, TYPE_SIZE is the size of the data
    memcpy(&x, in_bytes, sizeof(int));
	
	// since we consumed TYPE_SIZE number of bytes from the buffer, we set *in_len to TYPE_SIZE
    *in_len = sizeof(int);
	
    return true;
  }
};
*/

#define PRIMITIVE_TYPES \
X(int) \
X(char) \
X(short) \
X(long) \
X(long long) \
X(unsigned int) \
X(unsigned char) \
X(unsigned short) \
X(unsigned long) \
X(unsigned long long) \
X(bool) \
X(float) \
X(double)


#define X(e) \
template <> struct Protocol<e> { \
  static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const e &x) { \
    if (*out_len < sizeof(e)) return false;  \
    memcpy(out_bytes, &x, sizeof(e)); \
    *out_len = sizeof(e); \
    return true; \
  } \
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, e &x) { \
    if (*in_len < sizeof(e)) return false; \
    memcpy(&x, in_bytes, sizeof(e)); \
    *in_len = sizeof(e); \
    return true; \
  } \
};

PRIMITIVE_TYPES
#undef X

template <typename T>
unsigned int encodeSize(const T &x) {
  return sizeof(T);
}
template <>
unsigned int encodeSize(const std::string &x) {
  return x.size() + 1;
}
template <typename T>
unsigned int encodeSize(const std::vector<T> &x) {
  unsigned int size = 2;
  for(const auto& iter: x) {
    size += encodeSize(iter);
  }
  return size;
}

template <> struct Protocol<std::string> {
  static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const std::string &x) {
    if (*out_len < encodeSize(x)) return false; 
    for(int i=0; i<x.length(); i++){
      memcpy(&out_bytes[i],&x[i], 1);
    }
    out_bytes[x.length()] = (uint8_t)'\0';
    *out_len = encodeSize(x);
    return true;
  }
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, std::string &x) {
    int term_index = -1;
    for(int i=0; i<*in_len; i++){
      if((char)in_bytes[i] == '\0') {
        term_index = i;
        break;
      }
    }
    if(term_index == -1) return false;
    x = std::string((char *)in_bytes);
    *in_len = encodeSize(x);
    return true;
  }
};

template <typename T> struct Protocol<std::vector<T>> {
  static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const std::vector<T> &x) {
    if(*out_len < encodeSize(x)) return false;

    unsigned short vec_length = x.size();
    memcpy(out_bytes, &vec_length, sizeof(unsigned short));
    uint32_t remaining_len = *out_len - sizeof(unsigned short);
    
    for(const auto& iter : x) {
      unsigned int buffer_index = *out_len - remaining_len;
      uint32_t temp_len = remaining_len;
      Protocol<T>::Encode(out_bytes+buffer_index, &temp_len, iter);
      remaining_len -= temp_len;
    }
    *out_len = *out_len - remaining_len;
    return true;
  }
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, std::vector<T> &x) {
    if(*in_len < 2) return false;
    std::vector<T> res_vec;
    unsigned short vec_length;
    memcpy(&vec_length, in_bytes, sizeof(unsigned short));
    uint32_t remaining_len = *in_len - sizeof(unsigned short);
    for(int i = 0; i < vec_length; i++) {
      if(remaining_len <= 0) return false;
      unsigned int buffer_index = *in_len - remaining_len;
      uint32_t temp_len = remaining_len;
      T temp_value;
      if(!Protocol<T>::Decode(in_bytes+buffer_index, &temp_len, temp_value)) return false;
      res_vec.push_back(temp_value);
      remaining_len -= temp_len;
    }
    *in_len = *in_len - remaining_len;
    x.swap(res_vec);
    return true;
  }
  
};

// TASK2: Client-side
// class IntParam : public BaseParams {
//   int p;
//  public:
//   IntParam(int p) : p(p) {}

//   bool Encode(uint8_t *out_bytes, uint32_t *out_len) const override {
//     return Protocol<int>::Encode(out_bytes, out_len, p);
//   }
// };
class NoParam : public BaseParams {
 public:
  NoParam() {}

  bool Encode(uint8_t *out_bytes, uint32_t *out_len) const override {
    return true;
  }
};

// TASK2: Server-side
template <typename Svc, typename ReturnT>
class Procedure : public BaseProcedure {
  bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                        uint8_t *out_bytes, uint32_t *out_len) override final {
    // -------- x is a param --------
    // int x;
    // This function is similar to Decode. We need to return false if buffer
    // isn't large enough, or fatal error happens during parsing.
    /* if (!Protocol<ReturnT>::Decode(in_bytes, in_len, x)) {
       return false;
    } */
    // Now we cast the function pointer func_ptr to its original type.
    //
    // This incomplete solution only works for this type of member functions.
    using FunctionPointerType = ReturnT (Svc::*)();
    auto p = func_ptr.To<FunctionPointerType>();
    ReturnT result = (((Svc *) instance)->*p)();
    if (!Protocol<ReturnT>::Encode(out_bytes, out_len, result)) {
      // out_len should always be large enough so this branch shouldn't be
      // taken. However just in case, we return a fatal error here.
      return false;
    }
    return true;
  }
};
template <typename Svc>
class Procedure<Svc, void> : public BaseProcedure {
  bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                        uint8_t *out_bytes, uint32_t *out_len) override final {
    using FunctionPointerType = void (Svc::*)();
    auto p = func_ptr.To<FunctionPointerType>();
    (((Svc *) instance)->*p)();
    return true;
  }
};

// TASK2: Client-side
template <typename ReturnT>
class Result : public BaseResult {
  ReturnT r;
 public:
  bool HandleResponse(uint8_t *in_bytes, uint32_t *in_len) override final {
    return Protocol<ReturnT>::Decode(in_bytes, in_len, r);
  }
  ReturnT &data() { return r; }
};
template <>
class Result<void> : public BaseResult {
 public:
  bool HandleResponse(uint8_t *in_bytes, uint32_t *in_len) override final {
    return true;
  }
  void data() { return; }
};

// TASK2: Client-side
class Client : public BaseClient {
 public:
  template <typename Svc, typename ReturnT>
  Result<ReturnT> *Call(Svc *svc, ReturnT (Svc::*func)()) {
    // Lookup instance and function IDs.
    int instance_id = svc->instance_id();
    int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(func));

    auto result = new Result<ReturnT>();

    // We also send the paramters of the functions. For this incomplete
    // solution, it must be one integer.
    // if (!Send(instance_id, func_id, new IntParam(x), result)) {

    if (!Send(instance_id, func_id, new NoParam(), result)) {
      // Fail to send, then delete the result and return nullptr.
      delete result;
      return nullptr;
    }
    return result;
  }
};

// TASK2: Server-side
template <typename Svc>
class Service : public BaseService {
 protected:
  template <typename T>
  void Export(T (Svc::*func)()) {
    ExportRaw(MemberFunctionPtr::From(func), new Procedure<Svc, T>());
  }

  // void Export(int (Svc::*func)(int)) {
  //   ExportRaw(MemberFunctionPtr::From(func), new IntIntProcedure<Svc>());
  // }
  
};

}

#endif /* RPCXX_H */
