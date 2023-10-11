package graphvent

import (
  graphql "github.com/graphql-go/graphql"
  "github.com/google/uuid"
  "reflect"
  "fmt"
  "time"
)

type GQLTypeInfo struct {
  graphql.Type
  FromGQL func(interface{})(reflect.Value, error)
}

var kind_gql_map = map[reflect.Kind]GQLTypeInfo{
  reflect.String: {
    graphql.String,
    func(value interface{})(reflect.Value, error) {
      return reflect.ValueOf(value), nil
    },
  },
  reflect.Bool: {
    graphql.Boolean,
    func(value interface{})(reflect.Value, error) {
      return reflect.ValueOf(value), nil
    },
  },
}

var type_gql_map = map[reflect.Type]GQLTypeInfo{
  reflect.TypeOf(&NodeID{}): {
    graphql.String,
    func(value interface{}) (reflect.Value, error) {
      str, ok := value.(string)
      if ok == false {
        return reflect.Value{}, fmt.Errorf("value is not string")
      }

      if str == "" {
        return reflect.New(reflect.TypeOf(&NodeID{})).Elem(), nil
      }

      id_parsed, err := ParseID(str)
      if err != nil {
        return reflect.Value{}, err
      }

      return reflect.ValueOf(&id_parsed), nil
    },
  },
  reflect.TypeOf(NodeID{}): {
    graphql.String,
    func(value interface{})(reflect.Value, error) {
      str, ok := value.(string)
      if ok == false {
        return reflect.Value{}, fmt.Errorf("value is not string")
      }

      id_parsed, err := ParseID(str)
      if err != nil {
        return reflect.Value{}, err
      }

      return reflect.ValueOf(id_parsed), nil
    },
  },
}

type StructFieldInfo struct {
  Name string
  Type reflect.Type
  GQL *GQLTypeInfo
  Index []int
}

func SignalFromArgs(signal_type reflect.Type, fields []StructFieldInfo, args map[string]interface{}, id_index, direction_index []int) (Signal, error) {
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
    if field_value.CanConvert(field.Type) == false {
      return nil, fmt.Errorf("Arg %s wrong type %s/%s", field.Name, field_value.Type(), field.Type)
    }
    value, err := field.GQL.FromGQL(arg)
    if err != nil {
      return nil, err
    }
    field_value.Set(value)
  }
  return signal_value.Interface().(Signal), nil
}

func (ext *GQLExtContext) AddSignalMutation(name string, send_id_key string, signal_type reflect.Type) error {
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
          return fmt.Errorf("Signal has repeated tag %s", gv_tag)
        }
        var gql_info GQLTypeInfo
        var type_mapped bool
        gql_info, type_mapped = type_gql_map[field.Type]
        if type_mapped == false {
          var kind_mapped bool
          gql_info, kind_mapped = kind_gql_map[field.Type.Kind()]
          if kind_mapped == false {
            return fmt.Errorf("Signal has unsupported type/kind: %s/%s", field.Type, field.Type.Kind())
          }
        }

        args[gv_tag] = &graphql.ArgumentConfig{
          Type: gql_info.Type,
        }
        arg_info = append(arg_info, StructFieldInfo{
          gv_tag,
          field.Type,
          &gql_info,
          field.Index,
        })
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

    signal, err := SignalFromArgs(signal_type, arg_info, p.Args, id_index, direction_index)
    if err != nil {
      return nil, err
    }
    msgs := Messages{}
    msgs = msgs.Add(ctx.Context, ctx.User, ctx.Key, signal, send_id)

    response_chan := ctx.Ext.GetResponseChannel(signal.ID())
    err = ctx.Context.Send(msgs)
    if err != nil {
      ctx.Ext.FreeResponseChannel(signal.ID())
      return nil, err
    }

    response, err := WaitForResponse(response_chan, 100*time.Millisecond, signal.ID())
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

  ext.Mutation.AddFieldConfig(name, &graphql.Field{
    Type: graphql.String,
    Args: args,
    Resolve: resolve_signal,
  })
  return nil
}
