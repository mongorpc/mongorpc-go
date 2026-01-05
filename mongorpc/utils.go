package mongorpc

import (
	"fmt"
	"time"

	pb "github.com/mongorpc/mongorpc-go/gen/mongorpc/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// toProtoValue converts a Go value to a Proto Value.
func toProtoValue(v any) *pb.Value {
	if v == nil {
		return &pb.Value{ValueType: &pb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
	}

	switch val := v.(type) {
	case bool:
		return &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: val}}
	case int:
		return &pb.Value{ValueType: &pb.Value_Int64Value{Int64Value: int64(val)}}
	case int32:
		return &pb.Value{ValueType: &pb.Value_Int32Value{Int32Value: val}}
	case int64:
		return &pb.Value{ValueType: &pb.Value_Int64Value{Int64Value: val}}
	case float64:
		return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: val}}
	case string:
		return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: val}}
	case []byte:
		return &pb.Value{ValueType: &pb.Value_BytesValue{BytesValue: val}}
	case time.Time:
		return &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(val)}}
	case map[string]any:
		fields := make(map[string]*pb.Value)
		for k, v := range val {
			fields[k] = toProtoValue(v)
		}
		return &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: fields}}}
	case []any:
		values := make([]*pb.Value, len(val))
		for i, v := range val {
			values[i] = toProtoValue(v)
		}
		return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: values}}}
	case Document:
		return toProtoValue(map[string]any(val))
	default:
		// Fallback to string representation for unknown types
		return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: fmt.Sprintf("%v", val)}}
	}
}

// fromProtoValue converts a Proto Value to a Go value.
func fromProtoValue(v *pb.Value) any {
	if v == nil {
		return nil
	}

	switch t := v.ValueType.(type) {
	case *pb.Value_NullValue:
		return nil
	case *pb.Value_BooleanValue:
		return t.BooleanValue
	case *pb.Value_Int32Value:
		return int(t.Int32Value)
	case *pb.Value_Int64Value:
		return t.Int64Value
	case *pb.Value_DoubleValue:
		return t.DoubleValue
	case *pb.Value_StringValue:
		return t.StringValue
	case *pb.Value_BytesValue:
		return t.BytesValue
	case *pb.Value_TimestampValue:
		return t.TimestampValue.AsTime()
	case *pb.Value_ObjectIdValue:
		return t.ObjectIdValue.Hex
	case *pb.Value_ArrayValue:
		values := make([]any, len(t.ArrayValue.Values))
		for i, v := range t.ArrayValue.Values {
			values[i] = fromProtoValue(v)
		}
		return values
	case *pb.Value_MapValue:
		fields := make(map[string]any)
		for k, v := range t.MapValue.Fields {
			fields[k] = fromProtoValue(v)
		}
		return fields
	default:
		return nil
	}
}

// toProtoDocument converts a Go Document to a Proto Document.
func toProtoDocument(doc Document) *pb.Document {
	pbDoc := &pb.Document{
		Fields: make(map[string]*pb.Value),
	}

	for k, v := range doc {
		if k == "_id" {
			if idStr, ok := v.(string); ok {
				pbDoc.Id = &pb.ObjectId{Hex: idStr}
			}
			continue
		}
		pbDoc.Fields[k] = toProtoValue(v)
	}

	return pbDoc
}

// fromProtoDocument converts a Proto Document to a Go Document.
func fromProtoDocument(pbDoc *pb.Document) Document {
	if pbDoc == nil {
		return nil
	}

	doc := make(Document)
	if pbDoc.Id != nil {
		doc["_id"] = pbDoc.Id.Hex
	}

	for k, v := range pbDoc.Fields {
		doc[k] = fromProtoValue(v)
	}

	return doc
}

// toProtoUpdate converts a Go Update to a Proto UpdateSpec.
func toProtoUpdate(u Update) *pb.UpdateSpec {
	ops := &pb.UpdateOperators{}

	for k, v := range u {
		switch k {
		case "$set":
			if val, ok := v.(map[string]any); ok {
				ops.Set = make(map[string]*pb.Value)
				for f, fv := range val {
					ops.Set[f] = toProtoValue(fv)
				}
			} else if val, ok := v.(Document); ok {
				ops.Set = make(map[string]*pb.Value)
				for f, fv := range val {
					ops.Set[f] = toProtoValue(fv)
				}
			}
		case "$unset":
			if val, ok := v.(map[string]any); ok {
				for f := range val {
					ops.Unset = append(ops.Unset, f)
				}
			} else if val, ok := v.(Document); ok {
				for f := range val {
					ops.Unset = append(ops.Unset, f)
				}
			}
		case "$inc":
			if val, ok := v.(map[string]any); ok {
				ops.Inc = make(map[string]*pb.Value)
				for f, fv := range val {
					ops.Inc[f] = toProtoValue(fv)
				}
			} else if val, ok := v.(Document); ok {
				ops.Inc = make(map[string]*pb.Value)
				for f, fv := range val {
					ops.Inc[f] = toProtoValue(fv)
				}
			}
			// TODO: Add support for other operators ($min, $max, $mul, etc.)
		}
	}

	return &pb.UpdateSpec{
		UpdateType: &pb.UpdateSpec_Operators{Operators: ops},
	}
}

// toProtoFilter converts a Go Filter to a Proto Filter.
func toProtoFilter(f Filter) *pb.Filter {
	return &pb.Filter{
		FilterType: &pb.Filter_Raw{
			Raw: toProtoMapValue(map[string]any(f)),
		},
	}
}

// toProtoMapValue converts a Go map to a Proto MapValue.
func toProtoMapValue(m map[string]any) *pb.MapValue {
	fields := make(map[string]*pb.Value)
	for k, v := range m {
		fields[k] = toProtoValue(v)
	}
	return &pb.MapValue{Fields: fields}
}
