# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: store.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='store.proto',
  package='',
  syntax='proto3',
  serialized_pb=_b('\n\x0bstore.proto\"N\n\x0b\x43onsistency\x12!\n\x05level\x18\x01 \x01(\x0e\x32\x12.Consistency.Const\"\x1c\n\x05\x43onst\x12\x07\n\x03ONE\x10\x00\x12\n\n\x06QUORUM\x10\x01\"2\n\x06GetMsg\x12\x0b\n\x03key\x18\x01 \x01(\r\x12\x1b\n\x05level\x18\x02 \x01(\x0b\x32\x0c.Consistency\"1\n\x06PutMsg\x12\x0b\n\x03key\x18\x01 \x01(\r\x12\x0b\n\x03val\x18\x02 \x01(\t\x12\r\n\x05level\x18\x03 \x01(\r\"\x1a\n\x0bStringValue\x12\x0b\n\x03val\x18\x01 \x01(\t\"\x1a\n\x07Success\x12\x0f\n\x07success\x18\x01 \x01(\x08\" \n\x04Pair\x12\x0b\n\x03key\x18\x01 \x01(\r\x12\x0b\n\x03val\x18\x02 \x01(\t\"\x90\x01\n\x03Msg\x12\x16\n\x03put\x18\x01 \x01(\x0b\x32\x07.PutMsgH\x00\x12\x16\n\x03get\x18\x02 \x01(\x0b\x32\x07.GetMsgH\x00\x12\"\n\nstring_val\x18\x03 \x01(\x0b\x32\x0c.StringValueH\x00\x12\x15\n\x04pair\x18\x04 \x01(\x0b\x32\x05.PairH\x00\x12\x17\n\x03suc\x18\x05 \x01(\x0b\x32\x08.SuccessH\x00\x42\x05\n\x03msgb\x06proto3')
)



_CONSISTENCY_CONST = _descriptor.EnumDescriptor(
  name='Const',
  full_name='Consistency.Const',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='ONE', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='QUORUM', index=1, number=1,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=65,
  serialized_end=93,
)
_sym_db.RegisterEnumDescriptor(_CONSISTENCY_CONST)


_CONSISTENCY = _descriptor.Descriptor(
  name='Consistency',
  full_name='Consistency',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='level', full_name='Consistency.level', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _CONSISTENCY_CONST,
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=15,
  serialized_end=93,
)


_GETMSG = _descriptor.Descriptor(
  name='GetMsg',
  full_name='GetMsg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='GetMsg.key', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='level', full_name='GetMsg.level', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=95,
  serialized_end=145,
)


_PUTMSG = _descriptor.Descriptor(
  name='PutMsg',
  full_name='PutMsg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='PutMsg.key', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='val', full_name='PutMsg.val', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='level', full_name='PutMsg.level', index=2,
      number=3, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=147,
  serialized_end=196,
)


_STRINGVALUE = _descriptor.Descriptor(
  name='StringValue',
  full_name='StringValue',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='val', full_name='StringValue.val', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=198,
  serialized_end=224,
)


_SUCCESS = _descriptor.Descriptor(
  name='Success',
  full_name='Success',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='success', full_name='Success.success', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=226,
  serialized_end=252,
)


_PAIR = _descriptor.Descriptor(
  name='Pair',
  full_name='Pair',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='Pair.key', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='val', full_name='Pair.val', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=254,
  serialized_end=286,
)


_MSG = _descriptor.Descriptor(
  name='Msg',
  full_name='Msg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='put', full_name='Msg.put', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='get', full_name='Msg.get', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='string_val', full_name='Msg.string_val', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='pair', full_name='Msg.pair', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='suc', full_name='Msg.suc', index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
    _descriptor.OneofDescriptor(
      name='msg', full_name='Msg.msg',
      index=0, containing_type=None, fields=[]),
  ],
  serialized_start=289,
  serialized_end=433,
)

_CONSISTENCY.fields_by_name['level'].enum_type = _CONSISTENCY_CONST
_CONSISTENCY_CONST.containing_type = _CONSISTENCY
_GETMSG.fields_by_name['level'].message_type = _CONSISTENCY
_MSG.fields_by_name['put'].message_type = _PUTMSG
_MSG.fields_by_name['get'].message_type = _GETMSG
_MSG.fields_by_name['string_val'].message_type = _STRINGVALUE
_MSG.fields_by_name['pair'].message_type = _PAIR
_MSG.fields_by_name['suc'].message_type = _SUCCESS
_MSG.oneofs_by_name['msg'].fields.append(
  _MSG.fields_by_name['put'])
_MSG.fields_by_name['put'].containing_oneof = _MSG.oneofs_by_name['msg']
_MSG.oneofs_by_name['msg'].fields.append(
  _MSG.fields_by_name['get'])
_MSG.fields_by_name['get'].containing_oneof = _MSG.oneofs_by_name['msg']
_MSG.oneofs_by_name['msg'].fields.append(
  _MSG.fields_by_name['string_val'])
_MSG.fields_by_name['string_val'].containing_oneof = _MSG.oneofs_by_name['msg']
_MSG.oneofs_by_name['msg'].fields.append(
  _MSG.fields_by_name['pair'])
_MSG.fields_by_name['pair'].containing_oneof = _MSG.oneofs_by_name['msg']
_MSG.oneofs_by_name['msg'].fields.append(
  _MSG.fields_by_name['suc'])
_MSG.fields_by_name['suc'].containing_oneof = _MSG.oneofs_by_name['msg']
DESCRIPTOR.message_types_by_name['Consistency'] = _CONSISTENCY
DESCRIPTOR.message_types_by_name['GetMsg'] = _GETMSG
DESCRIPTOR.message_types_by_name['PutMsg'] = _PUTMSG
DESCRIPTOR.message_types_by_name['StringValue'] = _STRINGVALUE
DESCRIPTOR.message_types_by_name['Success'] = _SUCCESS
DESCRIPTOR.message_types_by_name['Pair'] = _PAIR
DESCRIPTOR.message_types_by_name['Msg'] = _MSG
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Consistency = _reflection.GeneratedProtocolMessageType('Consistency', (_message.Message,), dict(
  DESCRIPTOR = _CONSISTENCY,
  __module__ = 'store_pb2'
  # @@protoc_insertion_point(class_scope:Consistency)
  ))
_sym_db.RegisterMessage(Consistency)

GetMsg = _reflection.GeneratedProtocolMessageType('GetMsg', (_message.Message,), dict(
  DESCRIPTOR = _GETMSG,
  __module__ = 'store_pb2'
  # @@protoc_insertion_point(class_scope:GetMsg)
  ))
_sym_db.RegisterMessage(GetMsg)

PutMsg = _reflection.GeneratedProtocolMessageType('PutMsg', (_message.Message,), dict(
  DESCRIPTOR = _PUTMSG,
  __module__ = 'store_pb2'
  # @@protoc_insertion_point(class_scope:PutMsg)
  ))
_sym_db.RegisterMessage(PutMsg)

StringValue = _reflection.GeneratedProtocolMessageType('StringValue', (_message.Message,), dict(
  DESCRIPTOR = _STRINGVALUE,
  __module__ = 'store_pb2'
  # @@protoc_insertion_point(class_scope:StringValue)
  ))
_sym_db.RegisterMessage(StringValue)

Success = _reflection.GeneratedProtocolMessageType('Success', (_message.Message,), dict(
  DESCRIPTOR = _SUCCESS,
  __module__ = 'store_pb2'
  # @@protoc_insertion_point(class_scope:Success)
  ))
_sym_db.RegisterMessage(Success)

Pair = _reflection.GeneratedProtocolMessageType('Pair', (_message.Message,), dict(
  DESCRIPTOR = _PAIR,
  __module__ = 'store_pb2'
  # @@protoc_insertion_point(class_scope:Pair)
  ))
_sym_db.RegisterMessage(Pair)

Msg = _reflection.GeneratedProtocolMessageType('Msg', (_message.Message,), dict(
  DESCRIPTOR = _MSG,
  __module__ = 'store_pb2'
  # @@protoc_insertion_point(class_scope:Msg)
  ))
_sym_db.RegisterMessage(Msg)


# @@protoc_insertion_point(module_scope)