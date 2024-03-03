package graphvent

import (
  graphql "github.com/graphql-go/graphql"
  "github.com/google/uuid"
  "reflect"
  "fmt"
  "time"
)

type StructFieldInfo struct {
  Name string
  Type *TypeInfo
  Index []int
}

func ArgumentInfo(ctx *Context, field reflect.StructField, gv_tag string) (StructFieldInfo, error) {
  type_info, mapped := ctx.TypeReflects[field.Type]
  if mapped == false {
    return StructFieldInfo{}, fmt.Errorf("field %+v is of unregistered type %+v ", field.Name, field.Type)
  }
  
  return StructFieldInfo{
    Name: gv_tag,
    Type: type_info,
    Index: field.Index,
  }, nil
}

func SignalFromArgs(ctx *Context, signal_type reflect.Type, fields []StructFieldInfo, args map[string]interface{}, id_index, direction_index []int) (Signal, error) {
  fmt.Printf("FIELD: %+v\n", fields)
  signal_value := reflect.New(signal_type)

  id_field := signal_value.Elem().FieldByIndex(id_index)
  id_field.Set(reflect.ValueOf(uuid.New()))

  direction_field := signal_value.Elem().FieldByIndex(direction_index)
  direction_field.Set(reflect.ValueOf(Direct))

  for _, field := range(fields) {
    arg, arg_exists := args[field.Name]
    if arg_exists == false {
      return nil, fmt.Errorf("No arg provided named %s", field.Name)
    }
    field_value := signal_value.Elem().FieldByIndex(field.Index)
    if field_value.CanConvert(field.Type.Reflect) == false {
      return nil, fmt.Errorf("Arg %s wrong type %s/%s", field.Name, field_value.Type(), field.Type.Reflect)
    }
    value, err := field.Type.GQLValue(ctx, arg)
    if err != nil {
      return nil, err
    }
    fmt.Printf("Setting %s to %+v of type %+v\n", field.Name, value, value.Type())
    field_value.Set(value)
  }
  return signal_value.Interface().(Signal), nil
}

func NewSignalMutation(ctx *Context, name string, send_id_key string, signal_type reflect.Type) (*graphql.Field, error) {
  args := graphql.FieldConfigArgument{}
  arg_info := []StructFieldInfo{}
  var id_index []int = nil
  var direction_index []int = nil

  for _, field := range(reflect.VisibleFields(signal_type)) {
    gv_tag, tagged_gv := field.Tag.Lookup("gv")
    if tagged_gv {
      if gv_tag == "id" {
        id_index = field.Index
      } else if gv_tag == "direction" {
        direction_index = field.Index
      } else {
        _, exists := args[gv_tag]
        if exists == true {
          return nil, fmt.Errorf("Signal has repeated tag %s", gv_tag)
        } else {
          info, err := ArgumentInfo(ctx, field, gv_tag)
          if err != nil {
            return nil, err
          }
          args[gv_tag] = &graphql.ArgumentConfig{
          }
          arg_info = append(arg_info, info)
        }
      }
    }
  }

  _, send_exists := args[send_id_key]
  if send_exists == false {
    args[send_id_key] = &graphql.ArgumentConfig{
      Type: graphql.String,
    }
  }

  resolve_signal := func(p graphql.ResolveParams) (interface{}, error) {
    ctx, err := PrepResolve(p)
    if err != nil {
      return nil, err
    }

    send_id, err := ExtractID(p, send_id_key)
    if err != nil {
      return nil, err
    }

    signal, err := SignalFromArgs(ctx.Context, signal_type, arg_info, p.Args, id_index, direction_index)
    if err != nil {
      return nil, err
    }
    msgs := Messages{}
    msgs = msgs.Add(ctx.Context, send_id, ctx.Server, ctx.Authorization, signal)

    response_chan := ctx.Ext.GetResponseChannel(signal.ID())
    err = ctx.Context.Send(msgs)
    if err != nil {
      ctx.Ext.FreeResponseChannel(signal.ID())
      return nil, err
    }

    response, _, err := WaitForResponse(response_chan, 100*time.Millisecond, signal.ID())
    if err != nil {
      ctx.Ext.FreeResponseChannel(signal.ID())
      return nil, err
    }

    _, is_success := response.(*SuccessSignal)
    if is_success == true {
      return "success", nil
    }

    error_response, is_error := response.(*ErrorSignal)
    if is_error == true {
      return "error", fmt.Errorf(error_response.Error)
    }

    return nil, fmt.Errorf("response of unhandled type %s", reflect.TypeOf(response))
  }

  return &graphql.Field{
    Type: graphql.String,
    Args: args,
    Resolve: resolve_signal,
  }, nil
}
