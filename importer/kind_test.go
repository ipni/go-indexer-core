package importer

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAllKinds checks that AllKinds lists each Kind constant of kind.go,
// read from its source, once
func TestAllKinds(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "kind.go", nil, 0)
	require.NoError(t, err)
	var consts, listed []string
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			switch {
			case gen.Tok == token.CONST:
				if typ, ok := vs.Type.(*ast.Ident); ok && typ.Name == "Kind" {
					for _, name := range vs.Names {
						consts = append(consts, name.Name)
					}
				}
			case gen.Tok == token.VAR && vs.Names[0].Name == "AllKinds":
				for _, elt := range vs.Values[0].(*ast.CompositeLit).Elts {
					listed = append(listed, elt.(*ast.Ident).Name)
				}
			}
		}
	}
	require.NotEmpty(t, consts)
	require.ElementsMatch(t, consts, listed, "AllKinds and the Kind constants differ")
	require.Len(t, AllKinds, len(slices.Compact(slices.Sorted(slices.Values(AllKinds)))), "a kind repeats")
}
