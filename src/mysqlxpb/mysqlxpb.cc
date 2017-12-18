/*
 * Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0, as
 * published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an
 * additional permission to link the program and your derivative works
 * with the separately licensed software that they have included with
 * MySQL.
 *
 * Without limiting anything contained in the foregoing, this file,
 * which is part of MySQL Connector/Python, is also subject to the
 * Universal FOSS Exception, version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#include "python_cast.h"
#include "python.h"
#include "mysqlx/mysqlx.pb.h"
#include <google/protobuf/dynamic_message.h>
#include <stdexcept>
#include <cstring>
#include <string>


// This is a C++98-compatible class template to hold a pointer, which ensures
// that an object is deleted when a scope is left. For Python 2.7 compatibility
// on Windows C+11's unique_ptr can't be used. Once the Windows compiler is
// migrated this class should be replaced with unique_ptr for better
// maintenance.

template <typename T>
class MyScopedPtr {
  T *ptr;

  MyScopedPtr();
  MyScopedPtr(const MyScopedPtr &other); // make this non-construction-copyable
  MyScopedPtr &operator=(const MyScopedPtr &); // make this non-copyable
public:
  MyScopedPtr(T *ptr) : ptr(ptr) {}
  ~MyScopedPtr() { delete ptr; }

  operator bool() const {
    return ptr != NULL;
  }

  T& operator*() const {
    return *ptr;
  }

  T* operator->() const {
    return ptr;
  }
};

const char* kMessageTypeKey = "_mysqlxpb_type_name";


static PyObject* CreateMessage(const google::protobuf::Message& message);
static google::protobuf::Message* CreateMessage(PyObject* dict,
    google::protobuf::DynamicMessageFactory& factory);

static PyObject* ConvertPbToPyRequired(
    const google::protobuf::Message& message,
    const google::protobuf::FieldDescriptor& field) {
  switch (field.type()) {
    case google::protobuf::FieldDescriptor::TYPE_DOUBLE: {
      return PyFloat_FromDouble(
          message.GetReflection()->GetDouble(message, &field));
    }

    case google::protobuf::FieldDescriptor::TYPE_FLOAT: {
      return PyFloat_FromDouble(
          message.GetReflection()->GetFloat(message, &field));
    }

    case google::protobuf::FieldDescriptor::TYPE_INT64: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetInt64(message, &field)));
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT64: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetUInt64(message, &field)));
    }

    case google::protobuf::FieldDescriptor::TYPE_INT32: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetInt32(message, &field)));
    }

    case google::protobuf::FieldDescriptor::TYPE_FIXED64: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetUInt64(message, &field)));
    }

    case google::protobuf::FieldDescriptor::TYPE_FIXED32: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetUInt32(message, &field)));
    }

    case google::protobuf::FieldDescriptor::TYPE_BOOL: {
      return PyBool_FromLong(
          message.GetReflection()->GetBool(message, &field) ? 1 : 0);
    }

    case google::protobuf::FieldDescriptor::TYPE_STRING: {
      std::string str = message.GetReflection()->GetString(message, &field);
      return PyString_FromStringAndSize(str.c_str(), str.size());
    }

    case google::protobuf::FieldDescriptor::TYPE_MESSAGE: {
      return CreateMessage(
          message.GetReflection()->GetMessage(message, &field));
    }

    case google::protobuf::FieldDescriptor::TYPE_BYTES: {
      std::string str = message.GetReflection()->GetString(message, &field);
#ifdef PY3
      return PyBytes_FromStringAndSize(str.c_str(), str.size());
#else
      return PyString_FromStringAndSize(str.c_str(), str.size());
#endif
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT32: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetUInt32(message, &field)));
    }

    case google::protobuf::FieldDescriptor::TYPE_ENUM: {
      return PyLong_FromLong(
          message.GetReflection()->GetEnum(message, &field)->number());
    }

    case google::protobuf::FieldDescriptor::TYPE_SFIXED32: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetInt32(message, &field)));
    }

    case google::protobuf::FieldDescriptor::TYPE_SFIXED64: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetInt64(message, &field)));
    }

    case google::protobuf::FieldDescriptor::TYPE_SINT32: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetInt32(message, &field)));
    }

    case google::protobuf::FieldDescriptor::TYPE_SINT64: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetInt64(message, &field)));
    }
  }

  assert(false);
  return NULL;
}


static PyObject* ConvertPbToPyRepeated(int index,
    const google::protobuf::Message& message,
    const google::protobuf::FieldDescriptor& field) {
  switch (field.type()) {
    case google::protobuf::FieldDescriptor::TYPE_DOUBLE: {
      return PyFloat_FromDouble(message.GetReflection()->
          GetRepeatedDouble(message, &field, index));
    }

    case google::protobuf::FieldDescriptor::TYPE_FLOAT: {
      return PyFloat_FromDouble(message.GetReflection()->
          GetRepeatedFloat(message, &field, index));
    }

    case google::protobuf::FieldDescriptor::TYPE_INT64: {
      return PyLong_FromLong(static_cast<long>(message.
          GetReflection()->GetRepeatedInt64(message, &field, index)));
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT64: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetRepeatedUInt64(message, &field, index)));
    }

    case google::protobuf::FieldDescriptor::TYPE_INT32: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetRepeatedInt32(message, &field, index)));
    }

    case google::protobuf::FieldDescriptor::TYPE_FIXED64: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetRepeatedUInt64(message, &field, index)));
    }

    case google::protobuf::FieldDescriptor::TYPE_FIXED32: {
      return PyLong_FromLong(static_cast<long>(message.GetReflection()->
          GetRepeatedUInt32(message, &field, index)));
    }

    case google::protobuf::FieldDescriptor::TYPE_BOOL: {
      return PyBool_FromLong(message.GetReflection()->GetRepeatedBool(
          message, &field, index) ? 1 : 0);
    }

    case google::protobuf::FieldDescriptor::TYPE_STRING: {
      std::string str = message.GetReflection()->
          GetRepeatedString(message, &field, index);
      return PyString_FromStringAndSize(str.c_str(), str.size());
    }

    case google::protobuf::FieldDescriptor::TYPE_MESSAGE: {
      return CreateMessage(message.GetReflection()->
          GetRepeatedMessage(message, &field, index));
    }

    case google::protobuf::FieldDescriptor::TYPE_BYTES: {
      std::string str = message.GetReflection()->
          GetRepeatedString(message, &field, index);
#ifdef PY3
      return PyBytes_FromStringAndSize(str.c_str(), str.size());
#else
      return PyString_FromStringAndSize(str.c_str(), str.size());
#endif
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT32: {
      return PyLong_FromLong(message.GetReflection()->
          GetRepeatedUInt32(message, &field, index));
    }

    case google::protobuf::FieldDescriptor::TYPE_ENUM: {
      return PyLong_FromLong(message.GetReflection()->GetRepeatedEnum(
          message, &field, index)->number());
    }

    case google::protobuf::FieldDescriptor::TYPE_SFIXED32: {
      return PyLong_FromLong(static_cast<long>(message.
          GetReflection()->GetRepeatedInt32(message, &field, index)));
    }

    case google::protobuf::FieldDescriptor::TYPE_SFIXED64: {
      return PyLong_FromLong(static_cast<long>(message.
          GetReflection()->GetRepeatedInt64(message, &field, index)));
    }

    case google::protobuf::FieldDescriptor::TYPE_SINT32: {
      return PyLong_FromLong(static_cast<long>(message.
          GetReflection()->GetRepeatedInt32(message, &field, index)));
    }

    case google::protobuf::FieldDescriptor::TYPE_SINT64: {
      return PyLong_FromLong(static_cast<long>(message.
          GetReflection()->GetRepeatedInt64(message, &field, index)));
    }
  }

  assert(false);
  return NULL;
};


static void ConvertPyToPbRequired(
    google::protobuf::Message& message,
    const google::protobuf::FieldDescriptor& field,
    google::protobuf::DynamicMessageFactory& factory,
    PyObject* obj) {
  switch (field.type()) {
    case google::protobuf::FieldDescriptor::TYPE_DOUBLE: {
      message.GetReflection()->SetDouble(&message, &field,
          python_cast<double>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_FLOAT: {
      message.GetReflection()->SetFloat(&message, &field,
          python_cast<float>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_INT64: {
      message.GetReflection()->SetInt64(&message, &field,
          python_cast<google::protobuf::int64>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT64: {
      message.GetReflection()->SetUInt64(&message, &field,
          python_cast<google::protobuf::uint64>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_INT32: {
      message.GetReflection()->SetInt32(&message, &field,
          python_cast<google::protobuf::int32>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_FIXED64: {
      message.GetReflection()->SetUInt64(&message, &field,
          python_cast<google::protobuf::uint64>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_FIXED32: {
      message.GetReflection()->SetUInt32(&message, &field,
          python_cast<google::protobuf::uint32>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_BOOL: {
      message.GetReflection()->SetBool(&message, &field,
          python_cast<bool>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_STRING: {
      message.GetReflection()->SetString(&message, &field,
          python_cast<std::string>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_MESSAGE: {
      if (!PyDict_CheckExact(obj))
        throw std::invalid_argument("dict");
      message.GetReflection()->SetAllocatedMessage(&message,
          CreateMessage(obj, factory), &field);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_BYTES: {
      message.GetReflection()->SetString(&message, &field,
          python_cast<std::string>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT32: {
      message.GetReflection()->SetUInt32(&message, &field,
          python_cast<google::protobuf::uint32>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_ENUM: {
      message.GetReflection()->SetEnum(&message, &field,
          field.enum_type()->FindValueByNumber(
              python_cast<google::protobuf::int32>(obj)));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_SFIXED32: {
      message.GetReflection()->SetInt32(&message, &field,
          python_cast<google::protobuf::int32>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_SFIXED64: {
      message.GetReflection()->SetInt64(&message, &field,
          python_cast<google::protobuf::int64>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_SINT32: {
      message.GetReflection()->SetInt32(&message, &field,
          python_cast<google::protobuf::int32>(obj));
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_SINT64: {
      message.GetReflection()->SetInt64(&message, &field,
          python_cast<google::protobuf::int64>(obj));
      return;
    }
  }

  assert(false);
  throw std::runtime_error("Unknown Protobuf type.");
}


template<typename T>
static void AddPyListToMessageRepeatedField(
    google::protobuf::Message& message,
    const google::protobuf::FieldDescriptor& field,
    PyObject* list) {
  google::protobuf::RepeatedField<T>* mutable_field =
      message.GetReflection()->MutableRepeatedField<T>(&message, &field);
  Py_ssize_t list_size = PyList_Size(list);

  if (list_size > 0) {
    mutable_field->Reserve(list_size);
    for (Py_ssize_t idx = 0; idx < list_size; ++idx) {
      mutable_field->Add(python_cast<T>(PyList_GetItem(list, idx)));
    }
  }
}


static void AddPyListToMessageRepeatedMessage(
    google::protobuf::Message& message,
    const google::protobuf::FieldDescriptor& field,
    google::protobuf::DynamicMessageFactory& factory,
    PyObject* list) {
  google::protobuf::RepeatedPtrField<google::protobuf::Message>* mutable_field =
      message.GetReflection()->
      MutableRepeatedPtrField<google::protobuf::Message>(&message, &field);
  Py_ssize_t list_size = PyList_Size(list);

  if (list_size > 0) {
    mutable_field->Reserve(list_size);
    for (Py_ssize_t idx = 0; idx < list_size; ++idx) {
      mutable_field->AddAllocated(
        CreateMessage(PyList_GetItem(list, idx), factory));
    }
  }
}


static void AddPyListToMessageRepeatedString(
    google::protobuf::Message& message,
    const google::protobuf::FieldDescriptor& field,
    PyObject* list) {
  google::protobuf::RepeatedPtrField<google::protobuf::string>* mutable_field =
      message.GetReflection()->
      MutableRepeatedPtrField<google::protobuf::string>(&message, &field);
  Py_ssize_t list_size = PyList_Size(list);

  if (list_size > 0) {
    mutable_field->Reserve(list_size);
    for (Py_ssize_t idx = 0; idx < list_size; ++idx) {
      mutable_field->AddAllocated(new google::protobuf::string(
          python_cast<std::string>(PyList_GetItem(list, idx))));
    }
  }
}


static void AddPyListToMessageRepeatedEnum(
    google::protobuf::Message& message,
    const google::protobuf::FieldDescriptor& field,
    PyObject* list) {
  // TODO: Investigate if it is possible to preallocate repeated enum
  //       field in Protobuf (like in case of scalars and messages).
  Py_ssize_t list_size = PyList_Size(list);

  if (list_size > 0) {
    for (Py_ssize_t idx = 0; idx < list_size; ++idx) {
      google::protobuf::int32 enum_int_value =
          python_cast<google::protobuf::int32>(PyList_GetItem(list, idx));
      const google::protobuf::EnumValueDescriptor* enum_value =
          field.enum_type()->FindValueByNumber(enum_int_value);

      message.GetReflection()->SetRepeatedEnum(&message, &field, idx,
                                               enum_value);
    }
  }
}


static void ConvertPyToPbRepeated(
    google::protobuf::Message& message,
    const google::protobuf::FieldDescriptor& field,
    google::protobuf::DynamicMessageFactory& factory,
    PyObject* list) {
  switch (field.type()) {
    case google::protobuf::FieldDescriptor::TYPE_DOUBLE: {
      AddPyListToMessageRepeatedField<double>(message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_FLOAT: {
      AddPyListToMessageRepeatedField<float>(message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_INT64: {
      AddPyListToMessageRepeatedField<google::protobuf::int64>(
          message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT64: {
      AddPyListToMessageRepeatedField<google::protobuf::uint64>(
          message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_INT32: {
      AddPyListToMessageRepeatedField<google::protobuf::int32>(
          message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_FIXED64: {
      AddPyListToMessageRepeatedField<google::protobuf::uint64>(
          message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_FIXED32: {
      AddPyListToMessageRepeatedField<google::protobuf::uint32>(
          message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_BOOL: {
      AddPyListToMessageRepeatedField<bool>(message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_STRING: {
      AddPyListToMessageRepeatedString(message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_MESSAGE: {
      AddPyListToMessageRepeatedMessage(message, field, factory, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_BYTES: {
      AddPyListToMessageRepeatedString(message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT32: {
      AddPyListToMessageRepeatedField<google::protobuf::uint32>(
          message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_ENUM: {
      AddPyListToMessageRepeatedEnum(message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_SFIXED32: {
      AddPyListToMessageRepeatedField<google::protobuf::int32>(
          message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_SFIXED64: {
      AddPyListToMessageRepeatedField<google::protobuf::int64>(
          message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_SINT32: {
      AddPyListToMessageRepeatedField<google::protobuf::int32>(
          message, field, list);
      return;
    }

    case google::protobuf::FieldDescriptor::TYPE_SINT64: {
      AddPyListToMessageRepeatedField<google::protobuf::int64>(
          message, field, list);
      return;
    }
  }

  assert(false);
  throw std::runtime_error("Unknown Protobuf type.");
}


static const google::protobuf::Descriptor* MessageDescriptorByName(
    const char* name) {
  return google::protobuf::DescriptorPool::generated_pool()->
      FindMessageTypeByName(name);
}


static void PythonAddDict(PyObject* dict,
                          const google::protobuf::Message& message,
                          const google::protobuf::FieldDescriptor& field) {
  PyObject* obj = ConvertPbToPyRequired(message, field);

  if (!obj) {
    throw std::runtime_error(
      "Failed to convert message field to Python object: " +
      field.full_name());
  }

  PyDict_SetItemString(dict, field.name().c_str(), obj);
}


static void PythonAddList(PyObject* list, int index,
                          const google::protobuf::Message& message,
                          const google::protobuf::FieldDescriptor& field) {
  PyObject* obj = ConvertPbToPyRepeated(index, message, field);

  if (!obj) {
    throw std::runtime_error(
        "Failed to convert message field to Python object: " +
        field.full_name());
  }

  PyList_SetItem(list, index, obj);
}


static PyObject* CreateMessage(const google::protobuf::Message& message) {
  PyObject* dict = PyDict_New();
  const google::protobuf::Descriptor* descriptor = message.GetDescriptor();
  const google::protobuf::Reflection* reflection = message.GetReflection();

  try {
    PyDict_SetItemString(dict, kMessageTypeKey,
        PyString_FromString(descriptor->full_name().c_str()));

    for (int idx = 0; idx < descriptor->field_count(); ++idx) {
      const google::protobuf::FieldDescriptor* field = descriptor->field(idx);

      switch (field->label()) {
        case google::protobuf::FieldDescriptor::LABEL_REQUIRED: {
          PythonAddDict(dict, message, *field);
          break;
        }
        case google::protobuf::FieldDescriptor::LABEL_OPTIONAL: {
          if (reflection->HasField(message, field))
            PythonAddDict(dict, message, *field);
          break;
        }
        case google::protobuf::FieldDescriptor::LABEL_REPEATED: {
          int listSize = reflection->FieldSize(message, field);
          PyObject* list = PyList_New(listSize);

          for (int idx = 0; idx < listSize; ++idx)
            PythonAddList(list, idx, message, *field);
          PyDict_SetItemString(dict, field->name().c_str(), list);
          break;
        }
      }
    }
  } catch(std::exception& e) {
    Py_DECREF(dict);
    dict = NULL;
    PyErr_SetString(PyExc_RuntimeError, e.what());
  }

  return dict;
}


static google::protobuf::Message* CreateMessage(PyObject* dict,
  google::protobuf::DynamicMessageFactory& factory)
{
  google::protobuf::Message* message = NULL;

  if (PyDict_CheckExact(dict)) {
    PyObject* type_name_obj = PyDict_GetItemString(dict, kMessageTypeKey);

    if (type_name_obj && PyString_CheckExact(type_name_obj)) {
      char* type_name = PyString_AsString(type_name_obj);
      const google::protobuf::Descriptor* descriptor =
          MessageDescriptorByName(type_name);

      if (descriptor) {
        message = factory.GetPrototype(descriptor)->New();

        if (message) {
          try {
            PyObject* key;
            PyObject* value;
            Py_ssize_t pos = 0;

            while (PyDict_Next(dict, &pos, &key, &value)) {
              if (key && PyString_CheckExact(key)) {
                char* key_name = PyString_AsString(key);

                if (::strcmp(key_name, kMessageTypeKey) == 0)
                  continue;

                const google::protobuf::FieldDescriptor* field =
                    descriptor->FindFieldByName(key_name);

                switch (field->label()) {
                  case google::protobuf::FieldDescriptor::LABEL_OPTIONAL:
                  case google::protobuf::FieldDescriptor::LABEL_REQUIRED: {
                    ConvertPyToPbRequired(*message, *field, factory, value);
                    break;
                  }
                  case google::protobuf::FieldDescriptor::LABEL_REPEATED: {
                    ConvertPyToPbRepeated(*message, *field, factory, value);
                    break;
                  }
                }
              } else {
                PyErr_SetString(PyExc_RuntimeError,
                    "Invalid message key type, string expected.");
              }
            }
          } catch(...) {
            delete message;
            message = NULL;
            PyErr_Format(PyExc_RuntimeError,
                         "Failed to initialize a message: %s", type_name);
          }
        } else {
          PyErr_Format(PyExc_RuntimeError, "Failed to create a message: %s",
                       type_name);
        }
      } else {
        PyErr_Format(PyExc_RuntimeError, "Unknown message type name: %s",
                     type_name);
      }
    } else {
      PyErr_SetString(PyExc_RuntimeError,
                      "Message type information missing.");
    }
  } else {
    PyErr_SetString(PyExc_TypeError, "Dictionary type expected.");
  }

  return message;
}


static PyObject* NewMessageImpl(const char* type_name) {
  PyObject* result = NULL;
  const google::protobuf::Descriptor* descriptor =
      MessageDescriptorByName(type_name);

  if (descriptor) {
    google::protobuf::DynamicMessageFactory factory;

    result = CreateMessage(*factory.GetPrototype(descriptor));
  } else {
    PyErr_Format(PyExc_RuntimeError, "Unknown message type: %s", type_name);
  }

  return result;
}


static PyObject* NewMessage(PyObject* self, PyObject* args) {
  PyObject* result = NULL;
  const char* type_name;

  if (PyArg_ParseTuple(args, "s", &type_name))
    result = NewMessageImpl(type_name);

  return result;
}


static PyObject* ParseMessageImpl(const char* type_name,
                                  const char* message_data,
                                  int message_data_size) {
  PyObject* result = NULL;
  const google::protobuf::Descriptor* descriptor =
      MessageDescriptorByName(type_name);

  if (descriptor) {
    google::protobuf::DynamicMessageFactory dynamic_factory;
    MyScopedPtr<google::protobuf::Message> message(
        dynamic_factory.GetPrototype(descriptor)->New());

    if (message) {
      if (message->ParseFromArray(message_data, message_data_size)) {
        result = CreateMessage(*message);
      } else {
        PyErr_Format(PyExc_RuntimeError, "Failed to parse message: %s",
                     type_name);
      }
    } else {
      PyErr_Format(PyExc_RuntimeError, "Failed to create message: %s",
                   type_name);
    }
  } else {
    PyErr_Format(PyExc_RuntimeError, "Unknown message type: %s", type_name);
  }

  return result;
}


static PyObject* ParseMessage(PyObject* self, PyObject* args) {
  PyObject* result = NULL;
  const char* type_name;
  const char* data;
  int data_size;

  if (PyArg_ParseTuple(args, "ss#", &type_name, &data, &data_size))
    result = ParseMessageImpl(type_name, data, data_size);

  return result;
}


static const char* GetMessageNameByTypeId(Mysqlx::ServerMessages::Type type) {
  switch (type) {
    case Mysqlx::ServerMessages::OK: { return "Mysqlx.Ok"; }
    case Mysqlx::ServerMessages::ERROR: { return "Mysqlx.Error"; }
    case Mysqlx::ServerMessages::CONN_CAPABILITIES: {
      return "Mysqlx.Connection.Capabilities";
    }
    case Mysqlx::ServerMessages::SESS_AUTHENTICATE_CONTINUE: {
      return "Mysqlx.Session.AuthenticateContinue";
    }
    case Mysqlx::ServerMessages::SESS_AUTHENTICATE_OK: {
      return "Mysqlx.Session.AuthenticateOk";
    }
    case Mysqlx::ServerMessages::NOTICE: { return "Mysqlx.Notice.Frame"; }
    case Mysqlx::ServerMessages::RESULTSET_COLUMN_META_DATA: {
      return "Mysqlx.Resultset.ColumnMetaData";
    }
    case Mysqlx::ServerMessages::RESULTSET_ROW: {
      return "Mysqlx.Resultset.Row";
    }
    case Mysqlx::ServerMessages::RESULTSET_FETCH_DONE: {
      return "Mysqlx.Resultset.FetchDone";
    }
    // TODO: Unused, enable in the future.
    // case Mysqlx::ServerMessages::RESULTSET_FETCH_SUSPENDED: { return ""; }
    case Mysqlx::ServerMessages::RESULTSET_FETCH_DONE_MORE_RESULTSETS: {
      return "Mysqlx.Resultset.FetchDoneMoreResultsets";
    }
    case Mysqlx::ServerMessages::SQL_STMT_EXECUTE_OK: {
      return "Mysqlx.Sql.StmtExecuteOk";
    }
    case Mysqlx::ServerMessages::RESULTSET_FETCH_DONE_MORE_OUT_PARAMS: {
      return "Mysqlx.Resultset.FetchDoneMoreOutParams";
    }
  }

  assert(false);
  return NULL;
};


static PyObject* ParseServerMessage(PyObject* self, PyObject* args) {
  PyObject* result = NULL;
  int type;
  const char* message_data;
  int message_data_size;

  if (PyArg_ParseTuple(args, "is#", &type, &message_data, &message_data_size))
  {
    const char* type_name = GetMessageNameByTypeId(
        static_cast<Mysqlx::ServerMessages::Type>(type));

    if (type_name)
      result = ParseMessageImpl(type_name, message_data, message_data_size);
    else
      PyErr_Format(PyExc_RuntimeError, "Unknown message type id: %i", type);
  }

  return result;
}


static PyObject* SerializeMessage(PyObject* self, PyObject* args) {
  PyObject* result = NULL;
  PyObject* dict;
  google::protobuf::DynamicMessageFactory factory;

  if (PyArg_ParseTuple(args, "O", &dict)) {
    MyScopedPtr<google::protobuf::Message> message(
        CreateMessage(dict, factory));

    if (message) {
      std::string buffer = message->SerializeAsString();

#ifdef PY3
      result = PyBytes_FromStringAndSize(buffer.c_str(), buffer.size());
#else
      result = PyString_FromStringAndSize(buffer.c_str(), buffer.size());
#endif
    }
  }

  return result;
}


static PyObject* EnumValue(PyObject* self, PyObject* args) {
  PyObject* result = NULL;
  const char* enum_full_value_name;

  if (PyArg_ParseTuple(args, "s", &enum_full_value_name)) {
    const char* last_dot = std::strrchr(enum_full_value_name, '.');

    if (last_dot) {
      std::string enum_type_name(enum_full_value_name, last_dot);
      std::string enum_value_name(last_dot + 1);

      const google::protobuf::EnumDescriptor* enum_type =
          google::protobuf::DescriptorPool::generated_pool()->
          FindEnumTypeByName(enum_type_name);

      if (enum_type) {
        const google::protobuf::EnumValueDescriptor* enum_value =
            enum_type->FindValueByName(enum_value_name);

        if (enum_value) {
          result = PyLong_FromLong(enum_value->number());
        } else {
          PyErr_Format(PyExc_RuntimeError, "Unknown enum value: %s",
                       enum_full_value_name);
        }
      } else {
        PyErr_Format(PyExc_RuntimeError, "Unknown enum type: %s",
                     enum_type_name.c_str());
      }
    } else {
      PyErr_Format(PyExc_RuntimeError, "Invalid enum name: %s",
                   enum_full_value_name);
    }
  }

  return result;
}


PyMODINIT_FUNC
#ifdef PY3
PyInit__mysqlxpb() {
#else
init_mysqlxpb() {
#endif
  static const char* kModuleName = "_mysqlxpb";

  static PyMethodDef methods_definition[] = {
    { "new_message", NewMessage, METH_VARARGS, "Create a new message." },
    { "parse_message", ParseMessage, METH_VARARGS, "Parse a message." },
    { "parse_server_message", ParseServerMessage, METH_VARARGS,
      "Parse a server-side message." },
    { "serialize_message", SerializeMessage, METH_VARARGS,
      "Serialize a message." },
    { "enum_value", EnumValue, METH_VARARGS, "Get enum value." },
    { NULL, NULL, 0, NULL }
  };

#ifdef PY3
  static PyModuleDef module_definition = {
    PyModuleDef_HEAD_INIT,
    kModuleName,
    NULL,
    -1,
    methods_definition
  };

  return PyModule_Create(&module_definition);
#else
  Py_InitModule(kModuleName, methods_definition);
#endif
}
