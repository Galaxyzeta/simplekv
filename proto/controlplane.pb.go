// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.1
// source: proto/controlplane.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ServerRole int32

const (
	ServerRole_follower ServerRole = 0
	ServerRole_leader   ServerRole = 1
)

// Enum value maps for ServerRole.
var (
	ServerRole_name = map[int32]string{
		0: "follower",
		1: "leader",
	}
	ServerRole_value = map[string]int32{
		"follower": 0,
		"leader":   1,
	}
)

func (x ServerRole) Enum() *ServerRole {
	p := new(ServerRole)
	*p = x
	return p
}

func (x ServerRole) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ServerRole) Descriptor() protoreflect.EnumDescriptor {
	return file_proto_controlplane_proto_enumTypes[0].Descriptor()
}

func (ServerRole) Type() protoreflect.EnumType {
	return &file_proto_controlplane_proto_enumTypes[0]
}

func (x ServerRole) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ServerRole.Descriptor instead.
func (ServerRole) EnumDescriptor() ([]byte, []int) {
	return file_proto_controlplane_proto_rawDescGZIP(), []int{0}
}

type FetchLogRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CallerNodeName string `protobuf:"bytes,1,opt,name=callerNodeName,proto3" json:"callerNodeName,omitempty"`
	OffsetFrom     int64  `protobuf:"varint,2,opt,name=offsetFrom,proto3" json:"offsetFrom,omitempty"`
	Count          int64  `protobuf:"varint,3,opt,name=count,proto3" json:"count,omitempty"`
}

func (x *FetchLogRequest) Reset() {
	*x = FetchLogRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_controlplane_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FetchLogRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FetchLogRequest) ProtoMessage() {}

func (x *FetchLogRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_controlplane_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FetchLogRequest.ProtoReflect.Descriptor instead.
func (*FetchLogRequest) Descriptor() ([]byte, []int) {
	return file_proto_controlplane_proto_rawDescGZIP(), []int{0}
}

func (x *FetchLogRequest) GetCallerNodeName() string {
	if x != nil {
		return x.CallerNodeName
	}
	return ""
}

func (x *FetchLogRequest) GetOffsetFrom() int64 {
	if x != nil {
		return x.OffsetFrom
	}
	return 0
}

func (x *FetchLogRequest) GetCount() int64 {
	if x != nil {
		return x.Count
	}
	return 0
}

type FetchLogResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data        [][]byte      `protobuf:"bytes,1,rep,name=data,proto3" json:"data,omitempty"`
	LeaderHw    int64         `protobuf:"varint,2,opt,name=leaderHw,proto3" json:"leaderHw,omitempty"`
	LeaderEpoch int64         `protobuf:"varint,3,opt,name=leaderEpoch,proto3" json:"leaderEpoch,omitempty"`
	BaseResp    *BaseResponse `protobuf:"bytes,255,opt,name=baseResp,proto3" json:"baseResp,omitempty"`
}

func (x *FetchLogResponse) Reset() {
	*x = FetchLogResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_controlplane_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FetchLogResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FetchLogResponse) ProtoMessage() {}

func (x *FetchLogResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_controlplane_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FetchLogResponse.ProtoReflect.Descriptor instead.
func (*FetchLogResponse) Descriptor() ([]byte, []int) {
	return file_proto_controlplane_proto_rawDescGZIP(), []int{1}
}

func (x *FetchLogResponse) GetData() [][]byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *FetchLogResponse) GetLeaderHw() int64 {
	if x != nil {
		return x.LeaderHw
	}
	return 0
}

func (x *FetchLogResponse) GetLeaderEpoch() int64 {
	if x != nil {
		return x.LeaderEpoch
	}
	return 0
}

func (x *FetchLogResponse) GetBaseResp() *BaseResponse {
	if x != nil {
		return x.BaseResp
	}
	return nil
}

type CollectLeaderEpochAndOffsetRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MyLeaderEpoch int64 `protobuf:"varint,1,opt,name=myLeaderEpoch,proto3" json:"myLeaderEpoch,omitempty"`
}

func (x *CollectLeaderEpochAndOffsetRequest) Reset() {
	*x = CollectLeaderEpochAndOffsetRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_controlplane_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CollectLeaderEpochAndOffsetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CollectLeaderEpochAndOffsetRequest) ProtoMessage() {}

func (x *CollectLeaderEpochAndOffsetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_controlplane_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CollectLeaderEpochAndOffsetRequest.ProtoReflect.Descriptor instead.
func (*CollectLeaderEpochAndOffsetRequest) Descriptor() ([]byte, []int) {
	return file_proto_controlplane_proto_rawDescGZIP(), []int{2}
}

func (x *CollectLeaderEpochAndOffsetRequest) GetMyLeaderEpoch() int64 {
	if x != nil {
		return x.MyLeaderEpoch
	}
	return 0
}

type CollectLeaderEpochAndOffsetResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LeaderEpoch int64         `protobuf:"varint,1,opt,name=leaderEpoch,proto3" json:"leaderEpoch,omitempty"`
	Offset      int64         `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	BaseResp    *BaseResponse `protobuf:"bytes,255,opt,name=baseResp,proto3" json:"baseResp,omitempty"`
}

func (x *CollectLeaderEpochAndOffsetResponse) Reset() {
	*x = CollectLeaderEpochAndOffsetResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_controlplane_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CollectLeaderEpochAndOffsetResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CollectLeaderEpochAndOffsetResponse) ProtoMessage() {}

func (x *CollectLeaderEpochAndOffsetResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_controlplane_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CollectLeaderEpochAndOffsetResponse.ProtoReflect.Descriptor instead.
func (*CollectLeaderEpochAndOffsetResponse) Descriptor() ([]byte, []int) {
	return file_proto_controlplane_proto_rawDescGZIP(), []int{3}
}

func (x *CollectLeaderEpochAndOffsetResponse) GetLeaderEpoch() int64 {
	if x != nil {
		return x.LeaderEpoch
	}
	return 0
}

func (x *CollectLeaderEpochAndOffsetResponse) GetOffset() int64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *CollectLeaderEpochAndOffsetResponse) GetBaseResp() *BaseResponse {
	if x != nil {
		return x.BaseResp
	}
	return nil
}

var File_proto_controlplane_proto protoreflect.FileDescriptor

var file_proto_controlplane_proto_rawDesc = []byte{
	0x0a, 0x18, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x70,
	0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x10, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x22, 0x6f, 0x0a, 0x0f, 0x46, 0x65, 0x74, 0x63, 0x68, 0x4c, 0x6f, 0x67, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x26, 0x0a, 0x0e, 0x63, 0x61, 0x6c, 0x6c, 0x65, 0x72,
	0x4e, 0x6f, 0x64, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e,
	0x63, 0x61, 0x6c, 0x6c, 0x65, 0x72, 0x4e, 0x6f, 0x64, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1e,
	0x0a, 0x0a, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x46, 0x72, 0x6f, 0x6d, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x0a, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x46, 0x72, 0x6f, 0x6d, 0x12, 0x14,
	0x0a, 0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x63,
	0x6f, 0x75, 0x6e, 0x74, 0x22, 0x96, 0x01, 0x0a, 0x10, 0x46, 0x65, 0x74, 0x63, 0x68, 0x4c, 0x6f,
	0x67, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74,
	0x61, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x12, 0x1a, 0x0a,
	0x08, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x48, 0x77, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x08, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x48, 0x77, 0x12, 0x20, 0x0a, 0x0b, 0x6c, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x45, 0x70, 0x6f, 0x63, 0x68, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b,
	0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x45, 0x70, 0x6f, 0x63, 0x68, 0x12, 0x30, 0x0a, 0x08, 0x62,
	0x61, 0x73, 0x65, 0x52, 0x65, 0x73, 0x70, 0x18, 0xff, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x13,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x42, 0x61, 0x73, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x52, 0x08, 0x62, 0x61, 0x73, 0x65, 0x52, 0x65, 0x73, 0x70, 0x22, 0x4a, 0x0a,
	0x22, 0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x45, 0x70,
	0x6f, 0x63, 0x68, 0x41, 0x6e, 0x64, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x24, 0x0a, 0x0d, 0x6d, 0x79, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x45,
	0x70, 0x6f, 0x63, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0d, 0x6d, 0x79, 0x4c, 0x65,
	0x61, 0x64, 0x65, 0x72, 0x45, 0x70, 0x6f, 0x63, 0x68, 0x22, 0x91, 0x01, 0x0a, 0x23, 0x43, 0x6f,
	0x6c, 0x6c, 0x65, 0x63, 0x74, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x45, 0x70, 0x6f, 0x63, 0x68,
	0x41, 0x6e, 0x64, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x20, 0x0a, 0x0b, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x45, 0x70, 0x6f, 0x63, 0x68,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x45, 0x70,
	0x6f, 0x63, 0x68, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x30, 0x0a, 0x08, 0x62,
	0x61, 0x73, 0x65, 0x52, 0x65, 0x73, 0x70, 0x18, 0xff, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x13,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x42, 0x61, 0x73, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x52, 0x08, 0x62, 0x61, 0x73, 0x65, 0x52, 0x65, 0x73, 0x70, 0x2a, 0x26, 0x0a,
	0x0a, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x52, 0x6f, 0x6c, 0x65, 0x12, 0x0c, 0x0a, 0x08, 0x66,
	0x6f, 0x6c, 0x6c, 0x6f, 0x77, 0x65, 0x72, 0x10, 0x00, 0x12, 0x0a, 0x0a, 0x06, 0x6c, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x10, 0x01, 0x32, 0xcc, 0x01, 0x0a, 0x13, 0x43, 0x6f, 0x6e, 0x74, 0x72, 0x6f,
	0x6c, 0x50, 0x6c, 0x61, 0x6e, 0x65, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x3d, 0x0a,
	0x08, 0x46, 0x65, 0x74, 0x63, 0x68, 0x4c, 0x6f, 0x67, 0x12, 0x16, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x2e, 0x46, 0x65, 0x74, 0x63, 0x68, 0x4c, 0x6f, 0x67, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x17, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x46, 0x65, 0x74, 0x63, 0x68, 0x4c,
	0x6f, 0x67, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x76, 0x0a, 0x1b,
	0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x45, 0x70, 0x6f,
	0x63, 0x68, 0x41, 0x6e, 0x64, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x29, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x2e, 0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x4c, 0x65, 0x61, 0x64, 0x65,
	0x72, 0x45, 0x70, 0x6f, 0x63, 0x68, 0x41, 0x6e, 0x64, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x2a, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43,
	0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x45, 0x70, 0x6f, 0x63,
	0x68, 0x41, 0x6e, 0x64, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x22, 0x00, 0x42, 0x08, 0x5a, 0x06, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_controlplane_proto_rawDescOnce sync.Once
	file_proto_controlplane_proto_rawDescData = file_proto_controlplane_proto_rawDesc
)

func file_proto_controlplane_proto_rawDescGZIP() []byte {
	file_proto_controlplane_proto_rawDescOnce.Do(func() {
		file_proto_controlplane_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_controlplane_proto_rawDescData)
	})
	return file_proto_controlplane_proto_rawDescData
}

var file_proto_controlplane_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_proto_controlplane_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_proto_controlplane_proto_goTypes = []interface{}{
	(ServerRole)(0),                             // 0: proto.ServerRole
	(*FetchLogRequest)(nil),                     // 1: proto.FetchLogRequest
	(*FetchLogResponse)(nil),                    // 2: proto.FetchLogResponse
	(*CollectLeaderEpochAndOffsetRequest)(nil),  // 3: proto.CollectLeaderEpochAndOffsetRequest
	(*CollectLeaderEpochAndOffsetResponse)(nil), // 4: proto.CollectLeaderEpochAndOffsetResponse
	(*BaseResponse)(nil),                        // 5: proto.BaseResponse
}
var file_proto_controlplane_proto_depIdxs = []int32{
	5, // 0: proto.FetchLogResponse.baseResp:type_name -> proto.BaseResponse
	5, // 1: proto.CollectLeaderEpochAndOffsetResponse.baseResp:type_name -> proto.BaseResponse
	1, // 2: proto.ControlPlaneService.FetchLog:input_type -> proto.FetchLogRequest
	3, // 3: proto.ControlPlaneService.CollectLeaderEpochAndOffset:input_type -> proto.CollectLeaderEpochAndOffsetRequest
	2, // 4: proto.ControlPlaneService.FetchLog:output_type -> proto.FetchLogResponse
	4, // 5: proto.ControlPlaneService.CollectLeaderEpochAndOffset:output_type -> proto.CollectLeaderEpochAndOffsetResponse
	4, // [4:6] is the sub-list for method output_type
	2, // [2:4] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_proto_controlplane_proto_init() }
func file_proto_controlplane_proto_init() {
	if File_proto_controlplane_proto != nil {
		return
	}
	file_proto_base_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_proto_controlplane_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FetchLogRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_controlplane_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FetchLogResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_controlplane_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CollectLeaderEpochAndOffsetRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_controlplane_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CollectLeaderEpochAndOffsetResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_controlplane_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_controlplane_proto_goTypes,
		DependencyIndexes: file_proto_controlplane_proto_depIdxs,
		EnumInfos:         file_proto_controlplane_proto_enumTypes,
		MessageInfos:      file_proto_controlplane_proto_msgTypes,
	}.Build()
	File_proto_controlplane_proto = out.File
	file_proto_controlplane_proto_rawDesc = nil
	file_proto_controlplane_proto_goTypes = nil
	file_proto_controlplane_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// ControlPlaneServiceClient is the client API for ControlPlaneService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ControlPlaneServiceClient interface {
	FetchLog(ctx context.Context, in *FetchLogRequest, opts ...grpc.CallOption) (*FetchLogResponse, error)
	CollectLeaderEpochAndOffset(ctx context.Context, in *CollectLeaderEpochAndOffsetRequest, opts ...grpc.CallOption) (*CollectLeaderEpochAndOffsetResponse, error)
}

type controlPlaneServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewControlPlaneServiceClient(cc grpc.ClientConnInterface) ControlPlaneServiceClient {
	return &controlPlaneServiceClient{cc}
}

func (c *controlPlaneServiceClient) FetchLog(ctx context.Context, in *FetchLogRequest, opts ...grpc.CallOption) (*FetchLogResponse, error) {
	out := new(FetchLogResponse)
	err := c.cc.Invoke(ctx, "/proto.ControlPlaneService/FetchLog", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlPlaneServiceClient) CollectLeaderEpochAndOffset(ctx context.Context, in *CollectLeaderEpochAndOffsetRequest, opts ...grpc.CallOption) (*CollectLeaderEpochAndOffsetResponse, error) {
	out := new(CollectLeaderEpochAndOffsetResponse)
	err := c.cc.Invoke(ctx, "/proto.ControlPlaneService/CollectLeaderEpochAndOffset", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ControlPlaneServiceServer is the server API for ControlPlaneService service.
type ControlPlaneServiceServer interface {
	FetchLog(context.Context, *FetchLogRequest) (*FetchLogResponse, error)
	CollectLeaderEpochAndOffset(context.Context, *CollectLeaderEpochAndOffsetRequest) (*CollectLeaderEpochAndOffsetResponse, error)
}

// UnimplementedControlPlaneServiceServer can be embedded to have forward compatible implementations.
type UnimplementedControlPlaneServiceServer struct {
}

func (*UnimplementedControlPlaneServiceServer) FetchLog(context.Context, *FetchLogRequest) (*FetchLogResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FetchLog not implemented")
}
func (*UnimplementedControlPlaneServiceServer) CollectLeaderEpochAndOffset(context.Context, *CollectLeaderEpochAndOffsetRequest) (*CollectLeaderEpochAndOffsetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CollectLeaderEpochAndOffset not implemented")
}

func RegisterControlPlaneServiceServer(s *grpc.Server, srv ControlPlaneServiceServer) {
	s.RegisterService(&_ControlPlaneService_serviceDesc, srv)
}

func _ControlPlaneService_FetchLog_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FetchLogRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlPlaneServiceServer).FetchLog(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.ControlPlaneService/FetchLog",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlPlaneServiceServer).FetchLog(ctx, req.(*FetchLogRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlPlaneService_CollectLeaderEpochAndOffset_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CollectLeaderEpochAndOffsetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlPlaneServiceServer).CollectLeaderEpochAndOffset(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.ControlPlaneService/CollectLeaderEpochAndOffset",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlPlaneServiceServer).CollectLeaderEpochAndOffset(ctx, req.(*CollectLeaderEpochAndOffsetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _ControlPlaneService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "proto.ControlPlaneService",
	HandlerType: (*ControlPlaneServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "FetchLog",
			Handler:    _ControlPlaneService_FetchLog_Handler,
		},
		{
			MethodName: "CollectLeaderEpochAndOffset",
			Handler:    _ControlPlaneService_CollectLeaderEpochAndOffset_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/controlplane.proto",
}
