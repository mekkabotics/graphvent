package graphvent
import (
  "time"
  "reflect"
  "github.com/graphql-go/graphql"
  "github.com/graphql-go/graphql/language/ast"
)

func GetFieldNames(ctx *Context, selection_set *ast.SelectionSet) []string {
  names := []string{}
  if selection_set == nil {
    return names
  }

  for _, sel := range(selection_set.Selections) {
    switch field := sel.(type) {
    case *ast.Field:
      names = append(names, field.Name.Value)
    case *ast.InlineFragment:
      names = append(names, GetFieldNames(ctx, field.SelectionSet)...)
    default:
      ctx.Log.Logf("gql", "Unknown selection type: %s", reflect.TypeOf(field))
    }
  }

  return names
}

func GetResolveFields(ctx *Context, p graphql.ResolveParams) []string {
  names := []string{}
  for _, field := range(p.Info.FieldASTs) {
    names = append(names, GetFieldNames(ctx, field.SelectionSet)...)
  }

  return names
}

func ResolveNode(ctx *ResolveContext, p graphql.ResolveParams, id_str string) (NodeResult, error) {
  var zero NodeResult
  fields := GetResolveFields(ctx.Context, p)
  ctx.Context.Log.Logf("gql", "RESOLVE_NODE(%s): %+v", id_str, fields)

  id, err := ParseID(id_str)
  if err != nil {
    return zero, err
  }

  // Get a list of fields that will be written
  ext_fields, err := ctx.GQLContext.GetACLFields(p.Info.FieldName, fields)
  if err != nil {
    return zero, err
  }
  // Create a read signal, send it to the specified node, and add the wait to the response map if the send returns no error
  read_signal := NewReadSignal(ext_fields)

  ctx.Ext.resolver_reads_lock.Lock()
  ctx.Ext.resolver_reads[read_signal.UUID] = ctx.ID
  ctx.Ext.resolver_reads_lock.Unlock()

  err = ctx.Context.Send(ctx.Server.ID, id, read_signal)
  if err != nil {
    ctx.Ext.resolver_reads_lock.Lock()
    delete(ctx.Ext.resolver_reads, read_signal.UUID)
    ctx.Ext.resolver_reads_lock.Unlock()
    return zero, err
  }

  // Wait for the response, returning an error on timeout
  response, err := WaitForReadResult(ctx.Chan, time.Millisecond*100, read_signal.UUID)
  if err != nil {
    return zero, err
  }

  return NodeResult{id, response}, nil
}
