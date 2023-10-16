package graphvent

import (
  graphql "github.com/graphql-go/graphql"
  "github.com/google/uuid"
  "reflect"
  "fmt"
  "time"
)

type GQLTypeConverter func(*GQLExtContext, reflect.Type)(graphql.Type, error)
type GQLValueConverter func(*GQLExtContext, interface{})(reflect.Value, error)
type GQLTypeInfo struct {
  Type GQLTypeConverter
  Value GQLValueConverter
}

func GetGQLTypeInfo(ctx *GQLExtContext, reflect_type reflect.Type) (*GQLTypeInfo, error) {
  type_info, type_mapped := ctx.TypeMap[reflect_type]
  if type_mapped == false {
    kind_info, kind_mapped := ctx.KindMap[reflect_type.Kind()]
    if kind_mapped == false {
      return nil, fmt.Errorf("Signal has unsupported type/kind: %s/%s", reflect_type, reflect_type.Kind())
    } else {
      return &kind_info, nil
    }
  } else {
    return &type_info, nil
  }
}

type StructFieldInfo struct {
  Name string
  Type reflect.Type
  GQL *GQLTypeInfo
  Index []int
}

func SignalFromArgs(ctx *GQLExtContext, signal_type reflect.Type, fields []StructFieldInfo, args map[string]interface{}, id_index, direction_index []int) (Signal, error) {
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
    value, err := field.GQL.Value(ctx, arg)
    if err != nil {
      return nil, err
    }
    field_value.Set(value)
  }
  return signal_value.Interface().(Signal), nil
}

func ArgumentInfo(ctx *GQLExtContext, field reflect.StructField, gv_tag string) (*graphql.ArgumentConfig, StructFieldInfo, error) {
  gql_info, err := GetGQLTypeInfo(ctx, field.Type)
  if err != nil {
    return nil, StructFieldInfo{}, err
  }
  gql_type, err := gql_info.Type(ctx, field.Type)
  if err != nil {
    return nil, StructFieldInfo{}, err
  }
  return &graphql.ArgumentConfig{
    Type: gql_type,
  }, StructFieldInfo{
    gv_tag,
    field.Type,
    gql_info,
    field.Index,
  }, nil
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
        } else {
          config, info, err := ArgumentInfo(ext, field, gv_tag)
          if err != nil {
            return err
          }
          args[gv_tag] = config
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

    signal, err := SignalFromArgs(ctx.GQLContext, signal_type, arg_info, p.Args, id_index, direction_index)
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
