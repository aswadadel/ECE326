// -*- c++ -*-
#ifndef RPCXX_H
#define RPCXX_H

#include <cstdlib>
#include "rpc.h"
#include <iostream>
#include <cxxabi.h>

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
    for(decltype(x.length()) i=0; i<x.length(); i++){
      memcpy(&out_bytes[i],&x[i], 1);
    }
    out_bytes[x.length()] = (uint8_t)'\0';
    *out_len = encodeSize(x);
    return true;
  }
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, std::string &x) {
    int term_index = -1;
    for(uint32_t i=0; i<*in_len; i++){
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
    for(decltype(vec_length) i = 0; i < vec_length; i++) {
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
// CASE 1: handle single T-type param
template <typename ...ParamsT>
class Param : public BaseParams {
protected:
  bool Encode_recursive(uint8_t *out_bytes, uint32_t *out_len) const {
    return true;
  }
public:
  Param() {}

  bool Encode(uint8_t *out_bytes, uint32_t *out_len) const override {
    return true;
  }
};
template <typename T, typename ...ParamsT>
class Param<T, ParamsT...> : public Param<ParamsT...> {
  T p;
protected:
  bool Encode_recursive(uint8_t *out_bytes, uint32_t *out_len) const {
    uint32_t temp_out_len = *out_len;
    if(!Protocol<T>::Encode(out_bytes, &temp_out_len, p)) {
      std::cout << "Param: returning false" << std::endl;
      return false;
    }
    *out_len -= temp_out_len;
    return Param<ParamsT...>::Encode_recursive(out_bytes+temp_out_len, out_len);
  }
public:
  Param(T p, ParamsT ...params) : Param<ParamsT...>(params...), p(p) {}

  bool Encode(uint8_t *out_bytes, uint32_t *out_len) const override {
    uint32_t rem_out_len = *out_len;
    bool b = this->Encode_recursive(out_bytes, &rem_out_len);
    *out_len -= rem_out_len;
    if(!b) 
      std::cout << "Param: false" << std::endl;
    return b;
  }
};

// TASK2: Server-side
template <typename Svc, typename ReturnT, typename ... ParamT>
class Procedure : public BaseProcedure {
protected:
  template <typename ...ParamsT>
  bool DecodeAndExecute_recursive(uint8_t *in_bytes, uint32_t *in_len,
                        uint8_t *out_bytes, uint32_t *out_len, ParamsT ...params) {
    // std::cout << "base DecodeAndExecute_recursive" << std::endl;
    std::cout << "base DecodeAndExecute_recursive: " << *out_len << ", ";
    using FunctionPointerType = ReturnT (Svc::*)(ParamsT...);
    auto p = func_ptr.To<FunctionPointerType>();
    ReturnT result = (((Svc *) instance)->*p)(params...);
    if (!Protocol<ReturnT>::Encode(out_bytes, out_len, result)) {
      std::cout << "base DecodeAndExecute_recursive: FALSE" << std::endl;
      return false;
    }
    std::cout << *out_len << std::endl;
    return true;
  }
public:
  bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                        uint8_t *out_bytes, uint32_t *out_len) override {
    // std::cout << "start no params" << std::endl;
    std::cout << "start: ";
    std::cout << *in_len << ", " << *out_len << ".. ";
    std::cout << sizeof...(ParamT) << std::endl;

    *in_len = 0;

    return this->DecodeAndExecute_recursive(in_bytes, in_len, out_bytes, out_len);
  }
};

template <typename Svc>
class Procedure<Svc, void> : public BaseProcedure {
protected:
  template <typename ...ParamsT>
  bool DecodeAndExecute_recursive(uint8_t *in_bytes, uint32_t *in_len,
                        uint8_t *out_bytes, uint32_t *out_len, ParamsT ...params) {
    std::cout << "base void DecodeAndExecute_recursive" << std::endl;
    using FunctionPointerType = void (Svc::*)(ParamsT...);
    auto p = func_ptr.To<FunctionPointerType>();
    (((Svc *) instance)->*p)(params...);
    *out_len = 0;
    return true;
  }
public:
  bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                        uint8_t *out_bytes, uint32_t *out_len) override {
    // std::cout << "start no params" << std::endl;
    std::cout << "start: ";
    std::cout << *in_len << ", " << *out_len << ".. ";
    *in_len = 0;

    return this->DecodeAndExecute_recursive(in_bytes, in_len, out_bytes, out_len);
  }
};

template <typename Svc, typename ReturnT, typename P, typename ...ParamT>
class Procedure<Svc, ReturnT, P, ParamT...> : public Procedure<Svc, ReturnT, ParamT...> {
protected:
  template <typename ...ParamsT>
  bool DecodeAndExecute_recursive(uint8_t *in_bytes, uint32_t *in_len,
                        uint8_t *out_bytes, uint32_t *out_len, ParamsT ...params) {
    P x;
    int status = -4;
    std::cout << abi::__cxa_demangle(typeid(P).name(),0,0,&status) << std::endl;
    uint32_t temp_in_len = *in_len;
    if (!Protocol<P>::Decode(in_bytes, &temp_in_len, x)) {
      std::cout << "Procedure: returning false" << std::endl;
      return false;
    }
    *in_len -= temp_in_len;
    std::cout << *in_len << ", " << *out_len << ".. ";
    std::cout << sizeof...(ParamsT)+1
    << ", " << sizeof...(ParamT) << std::endl;
    
    return Procedure<Svc, ReturnT, ParamT...>
    ::DecodeAndExecute_recursive(in_bytes+temp_in_len, in_len, out_bytes, out_len, params..., x);
  }
public:
  bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                        uint8_t *out_bytes, uint32_t *out_len) override {
    int status = -4;

    std::cout << "start: ";
    std::cout << *in_len << ", " << *out_len << ".. ";
    std::cout << 0 << ", " << sizeof...(ParamT)+1 << std::endl;

    std::cout << "Return:" << std::endl;
    std::cout << abi::__cxa_demangle(typeid(ReturnT).name(),0,0,&status) << std::endl;

    std::cout << "Params:" << std::endl;


    uint32_t rem_in_len = *in_len;
    bool ret = this->DecodeAndExecute_recursive(in_bytes, &rem_in_len, out_bytes, out_len);
    *in_len -= rem_in_len;
    std::cout << *in_len << ", " << *out_len << std::endl;
    return ret;
  }
};

// TASK2: Client-side
// CASE 1: Handle any (except void) return type
template <typename ReturnT>
class Result : public BaseResult {
  ReturnT r;
 public:
  bool HandleResponse(uint8_t *in_bytes, uint32_t *in_len) override final {
    return Protocol<ReturnT>::Decode(in_bytes, in_len, r);
  }
  ReturnT &data() { return r; }
};
// CASE 2: handle void return type
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
//  CASE 1: ReturnT func(ParamT)
  template <typename Svc, typename ReturnT, typename ...ParamT>
  Result<ReturnT> *Call(Svc *svc, ReturnT (Svc::*func)(ParamT...), ParamT ...params) {
    // Lookup instance and function IDs.
    std::cout << "Begin Client::Call" << std::endl;
    int instance_id = svc->instance_id();
    int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(func));

    auto result = new Result<ReturnT>();

    // ---------  handle params here ---------
    if (!Send(instance_id, func_id, new Param<ParamT...>(params...), result)) {
      // Fail to send, then delete the result and return nullptr.
      std::cout << "Client::Call returns null" << std::endl;
      delete result;
      return nullptr;
    }
    std::cout << "End Client::Call" << std::endl;
    return result;
  }
};

// TASK2: Server-side
template <typename Svc>
class Service : public BaseService {
 protected:

  template <typename ReturnT, typename ...ParamT>
  void Export(ReturnT (Svc::*func)(ParamT...)) {
    ExportRaw(MemberFunctionPtr::From(func), new Procedure<Svc, ReturnT, ParamT...>());
  }
};

}

#endif /* RPCXX_H */
