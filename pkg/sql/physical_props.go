// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// physicalProps describes the physical properties of a set of results:
// equivalent columns, ordering information, etc.
//
// The intention is that this will evolve towards what is described in
// "Fundamental techniques for order optimization" by Simmen et al.
//
// == Equivalency groups ==
//
// Columns which we know are always equal on any row are equivalent; this
// information can come from filter and join conditions. This information is
// stored in a disjoint sets data structure.
//
// Note that in general, equality doesn't necessarily mean the values are
// identical/interchangeable (e.g. collated strings).
//
// == Constant columns ==
//
// If results are known to be restricted to a single value on some columns, we
// call these "constant columns". Note that if a column is constrained to be
// NULL, we don't consider that a constant column (technically: for a column to
// be constant, all rows must be equal on that column, which is not the case for
// NULLs).
//
// TODO(radu): generalize this to Functional Dependencies as described in the
// paper (referenced above).
//
// == Not-null columns ==
//
// We keep track of the columns for which we know there are no NULLs in the
// result set.
//
// == Keys ==
//
// A set of columns S forms a "key" if:
//  - no two rows are equal when projected on S, and
//  - no column in the set contains NULL values.
//
// The simplest example of a key is the primary key for a table (recall
// that all of the columns of the primary key are defined to be NOT NULL).
//
// A set of columns S forms a "weak key" if only condition (1) is satisfied. A
// UNIQUE index on a table is a weak key and possibly a key if all of the
// columns are NOT NULL. A weak key becomes a key when we can infer that all the
// columns in the set are not-NULL.
//
// We store a list of sets which form weak keys. In most cases there is at most
// one weak key. Some of these sets may actually be keys - namely, those which
// are a subset of the not-null column set.
//
// == Ordering information ==
//
// Typically the ordering information is used to see to what extent it can
// satisfy a "desired" ordering (which is just a list of columns and
// directions).
//
// In its simplest form, an ordering is simply a list of columns and a direction
// for each column, for example a+,b-,c+. This indicates that the rows are
// ordered by the value on column a (ascending); rows that have the same value
// on a are ordered by column b (descending); and rows that have the same values
// on a and b are ordered by column c (ascending).
//
// Ordering interacts with the other properties:
//  - constant columns are inconsequential w.r.t. ordering; for example, if an
//    index was defined on columns (a, b, c, d) and the WHERE clause was
//    "(a, c) = (1, 2)" then a and c are constant columns and we have an
//    ordering by b+ then d+. Such an ordering satisfies any of the following
//    desired orderings (among many others):
//      a+,c+
//      a-,c+
//      b+,a+,c+
//      b+,c+,a-
//      a+,b+,c+
//      c-,b+,a+
//      b+,d+,a-
//      a+,b+,c+,d+
//      b+,a+,c-,d+
//
//  - equivalency groups: a column in the ordering represents the entire group
//    of equivalent columns; any column in that group can be substituted in the
//    ordering. It's illegal for an ordering to contain multiple columns from
//    the same group.
//
//  - keys: if we have a key on columns (a, b) and the results have the ordering
//    a+,b+ we can satisfy any desired ordering that has a+,b+ as a prefix (such
//    as a+,b+,c-,d+).
type physicalProps struct {
	// column equivalency groups. This structure assigns a "representative" for
	// each group, which is the smallest column in that group (returned by Find);
	// only representatives can appear in the other fields below.
	eqGroups util.UnionFind

	// columns for which we know we have a single value. For groups of equivalent
	// columns, only the group representative can be in this set.
	constantCols util.FastIntSet

	// columns for which we know we have only non-NULL values. For groups of
	// equivalent columns, only the group representative can be in this set.
	// This set contains all the constant columns.
	notNullCols util.FastIntSet

	// List of column sets which are "weak keys" (see above). Key sets cannot
	// contain constant columns. A weak key set cannot contain any other weak key
	// set (it would be redundant).
	//
	// Some weak keys may in fact be keys, specifically those which are a subset
	// of notNullCols.
	//
	// An empty key set is valid (an empty key it implies there is at most one
	// row).
	weakKeys []util.FastIntSet

	// ordering of any other columns. This order is "reduced", meaning that there
	// the columns in constantCols do not appear in this ordering and for groups of
	// equivalent columns, only the group representative can appear.
	ordering sqlbase.ColumnOrdering
}

// check verifies the invariants of the structure.
func (pp physicalProps) check() {
	// Only equivalency group representatives show up in constantCols.
	for c, ok := pp.constantCols.Next(0); ok; c, ok = pp.constantCols.Next(c + 1) {
		if repr := pp.eqGroups.Find(c); repr != c {
			panic(fmt.Sprintf("non-representative const column %d (representative: %d)", c, repr))
		}
	}
	// Only equivalency group representatives show up in weakKeys.
	for _, k := range pp.weakKeys {
		for c, ok := k.Next(0); ok; c, ok = k.Next(c + 1) {
			if repr := pp.eqGroups.Find(c); repr != c {
				panic(fmt.Sprintf("non-representative key set column %d (representative: %d)", c, repr))
			}
			if pp.constantCols.Contains(c) {
				panic(fmt.Sprintf("const column %d in key set %s", c, k))
			}
		}
	}
	// Constant columns are by definition not-null.
	if !pp.constantCols.SubsetOf(pp.notNullCols) {
		panic(fmt.Sprintf("const columns %s should be not-null %s", pp.constantCols, pp.notNullCols))
	}
	var seen util.FastIntSet
	for _, o := range pp.ordering {
		if pp.isKey(seen) {
			panic(fmt.Sprintf("ordering contains columns after forming a key"))
		}
		// Only equivalency group representatives show up in ordering.
		if repr := pp.eqGroups.Find(o.ColIdx); repr != o.ColIdx {
			panic(fmt.Sprintf("non-representative order column %d (representative: %d)", o.ColIdx, repr))
		}
		// The ordering should not contain any constant or redundant columns.
		if pp.constantCols.Contains(o.ColIdx) {
			panic(fmt.Sprintf("const column %d appears in ordering", o.ColIdx))
		}
		if seen.Contains(o.ColIdx) {
			panic(fmt.Sprintf("duplicate column %d appears in ordering", o.ColIdx))
		}
		seen.Add(o.ColIdx)
	}
}

// Returns true if the given columns form a key. This is the case when there is
// a keySet that is a subset of cols and which has only not-null columns.
// Assumes cols contains only column group representatives.
func (pp *physicalProps) isKey(cols util.FastIntSet) bool {
	for _, k := range pp.weakKeys {
		if k.SubsetOf(cols) && k.SubsetOf(pp.notNullCols) {
			return true
		}
	}
	return false
}

// reduce rewrites an order specification, replacing columns with the
// equivalency group representative and removing any columns that are redundant
// Note: the resulting slice can be aliased with the given slice.
//
// An example of a redundant column is if we have an order A+,B+,C+ but A and C
// are in an equivalence group; the reduced ordering is A+,B+.
func (pp *physicalProps) reduce(order sqlbase.ColumnOrdering) sqlbase.ColumnOrdering {
	// We only allocate the result if we need to make modifications.
	var result sqlbase.ColumnOrdering

	// Set of column groups seen so far.
	var groupsSeen util.FastIntSet
	for i, o := range order {
		group := pp.eqGroups.Find(o.ColIdx)
		if pp.isKey(groupsSeen) {
			// The group of columns we added so far contains a key; further columns
			// are all redundant.
			if result == nil {
				return order[:i]
			}
			return result
		}
		redundant := groupsSeen.Contains(group) || pp.constantCols.Contains(group)
		groupsSeen.Add(group)
		if result == nil {
			if !redundant && o.ColIdx == group {
				// No modification necessary, continue.
				continue
			}
			result = make(sqlbase.ColumnOrdering, i, len(order))
			copy(result, order[:i])
		}
		if redundant {
			continue
		}
		o.ColIdx = group
		result = append(result, o)
	}
	if result == nil {
		// No modifications were necessary.
		return order
	}
	return result
}

// Format pretty-prints the physicalProps to a stream.
// If columns is not nil, column names are printed instead of column indexes.
//
// The output is a series of information "groups" separated by semicolons; each
// group shows:
//  - an equivalency group (e.g. a=b=c)
//  - a constant column (e.g. a=CONST)
//  - ordering (e.g. a+,b-)
//  - key (e.g. key(a,b) or weak-key(a,b))
//  - a not-null column (e.g. a!=NULL)
//
// Example:
//   a=b=c; d=e=f; g=CONST; h=CONST; b+,d-
func (pp *physicalProps) Format(buf *bytes.Buffer, columns sqlbase.ResultColumns) {
	pp.check()
	printCol := func(buf *bytes.Buffer, columns sqlbase.ResultColumns, colIdx int) {
		if columns == nil || colIdx >= len(columns) {
			fmt.Fprintf(buf, "@%d", colIdx+1)
		} else {
			fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
			fmtCtx.FormatNameP(&columns[colIdx].Name)
			_, _ = fmtCtx.WriteTo(buf)
			fmtCtx.Close()
		}
	}

	// Print any equivalency groups.
	var groups util.FastIntSet
	for i := 0; i < pp.eqGroups.Len(); i++ {
		representative := pp.eqGroups.Find(i)
		if representative != i {
			// We found a multi-column group.
			groups.Add(representative)
		}
	}

	firstGroup := true
	semiColon := func() {
		if !firstGroup {
			buf.WriteString("; ")
		}
		firstGroup = false
	}
	for r, ok := groups.Next(0); ok; r, ok = groups.Next(r + 1) {
		semiColon()
		// The representative is always the first element in the group.
		printCol(buf, columns, r)
		for i := r + 1; i < pp.eqGroups.Len(); i++ {
			if pp.eqGroups.Find(i) == r {
				buf.WriteByte('=')
				printCol(buf, columns, i)
			}
		}
	}
	// Print the constant columns.
	if !pp.constantCols.Empty() {
		for _, c := range pp.constantCols.Ordered() {
			semiColon()
			printCol(buf, columns, c)
			buf.WriteString("=CONST")
		}
	}
	// Print the non-null columns (except constant columns).
	if !pp.notNullCols.Empty() {
		for _, c := range pp.notNullCols.Ordered() {
			if !pp.constantCols.Contains(c) {
				semiColon()
				printCol(buf, columns, c)
				buf.WriteString("!=NULL")
			}
		}
	}

	for _, k := range pp.weakKeys {
		semiColon()
		if !k.SubsetOf(pp.notNullCols) {
			buf.WriteString("weak-")
		}

		buf.WriteString("key(")
		first := true
		for c, ok := k.Next(0); ok; c, ok = k.Next(c + 1) {
			if !first {
				buf.WriteByte(',')
			}
			first = false
			printCol(buf, columns, c)
		}
		buf.WriteByte(')')
	}

	// Print the ordering columns and for each their sort order.
	for i, o := range pp.ordering {
		if i == 0 {
			semiColon()
		} else {
			buf.WriteByte(',')
		}

		// We print the representative column of the group.
		prefix := byte('+')
		if o.Direction == encoding.Descending {
			prefix = byte('-')
		}
		buf.WriteByte(prefix)
		printCol(buf, columns, o.ColIdx)
	}
}

// AsString pretty-prints the physicalProps to a string. The result columns are
// used for printing column names and are optional.
func (pp physicalProps) AsString(columns sqlbase.ResultColumns) string {
	var buf bytes.Buffer
	pp.Format(&buf, columns)
	return buf.String()
}

func (pp *physicalProps) isEmpty() bool {
	return pp.constantCols.Empty() && len(pp.ordering) == 0
}

func (pp *physicalProps) addConstantColumn(colIdx int) {
	group := pp.eqGroups.Find(colIdx)
	pp.constantCols.Add(group)
	pp.notNullCols.Add(group)
	for i := range pp.weakKeys {
		pp.weakKeys[i].Remove(group)
	}
	pp.ordering = pp.reduce(pp.ordering)
}

func (pp *physicalProps) addNotNullColumn(colIdx int) {
	pp.notNullCols.Add(pp.eqGroups.Find(colIdx))
}

func (pp *physicalProps) addEquivalency(colA, colB int) {
	gA := pp.eqGroups.Find(colA)
	gB := pp.eqGroups.Find(colB)
	if gA == gB {
		return
	}
	pp.eqGroups.Union(gA, gB)
	// Make sure gA is the new representative.
	if pp.eqGroups.Find(gA) == gB {
		gA, gB = gB, gA
	}

	if pp.constantCols.Contains(gB) {
		pp.constantCols.Remove(gB)
		pp.constantCols.Add(gA)
	}

	if pp.notNullCols.Contains(gB) {
		pp.notNullCols.Remove(gB)
		pp.notNullCols.Add(gA)
	}

	if pp.constantCols.Contains(gA) {
		// One of the columns became a constant; remove it from all keys (similar to
		// what addConstantColumn does).
		for i := range pp.weakKeys {
			pp.weakKeys[i].Remove(gA)
			pp.weakKeys[i].Remove(gB)
		}
	} else {
		// Replace any occurrences of gB with gA (the new representative).
		for i := range pp.weakKeys {
			if pp.weakKeys[i].Contains(gB) {
				pp.weakKeys[i].Remove(gB)
				pp.weakKeys[i].Add(gA)
			}
		}
	}

	pp.ordering = pp.reduce(pp.ordering)
}

// addWeakKey adds the set columns specified by cols as a weak key.
// If it is a subset of another existing weak key, it will remove the
// existing weak key.
// addWeakKey also reduces ordering if necessary.
// cols is not mutated and safe for re-use.
func (pp *physicalProps) addWeakKey(cols util.FastIntSet) {
	// Remap column indices to equivalency group representatives.
	var k util.FastIntSet
	for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
		group := pp.eqGroups.Find(c)
		if !pp.constantCols.Contains(group) {
			k.Add(pp.eqGroups.Find(c))
		}
	}
	cols = k

	// Check if the key set is redundant, or if it makes some existing
	// key sets redundant.
	// Note: we don't use range because we are modifying weakKeys.
	for i := 0; i < len(pp.weakKeys); i++ {
		k := pp.weakKeys[i]
		if k.SubsetOf(cols) {
			// We already have a key with a subset of these columns.
			return
		}
		if cols.SubsetOf(k) {
			// The new key set makes this one redundant.
			copy(pp.weakKeys[i:], pp.weakKeys[i+1:])
			pp.weakKeys = pp.weakKeys[:len(pp.weakKeys)-1]
			i--
		}
	}
	pp.weakKeys = append(pp.weakKeys, cols)

	pp.ordering = pp.reduce(pp.ordering)
}

func (pp *physicalProps) addOrderColumn(colIdx int, dir encoding.Direction) {
	pp.ordering = append(pp.ordering, sqlbase.ColumnOrderInfo{
		ColIdx:    pp.eqGroups.Find(colIdx),
		Direction: dir,
	})
	pp.ordering = pp.reduce(pp.ordering)
}

// copy returns a copy of pp which can be modified independently.
func (pp *physicalProps) copy() physicalProps {
	result := physicalProps{
		eqGroups:     pp.eqGroups.Copy(),
		constantCols: pp.constantCols.Copy(),
		notNullCols:  pp.notNullCols.Copy(),
		weakKeys:     make([]util.FastIntSet, len(pp.weakKeys)),
	}
	for i := range pp.weakKeys {
		result.weakKeys[i] = pp.weakKeys[i].Copy()
	}

	if len(pp.ordering) > 0 {
		result.ordering = append(sqlbase.ColumnOrdering(nil), pp.ordering...)
	}
	return result
}

// reverse returns the physical properties for the reversed result set, which
// are the same except that the ordering directions are reversed.
func (pp *physicalProps) reverse() physicalProps {
	result := pp.copy()
	for i := range pp.ordering {
		result.ordering[i].Direction = result.ordering[i].Direction.Reverse()
	}
	return result
}

// project returns an physicalProps for a set of columns that include a
// projection of the original columns; the primary use case is computing an
// physicalProps for a renderNode.
//
// The new physicalProps refers to columns [0, len(colMap)); column i in the new
// physicalProps corresponds to column colMap[i] in the original physicalProps.
//
// For example, consider a table t with columns
//   0: A
//   1: B
//   2: C
//   3: D
// For the projection required by "SELECT B, D, C FROM t", colMap is {1, 3, 2}.
// If this table has (for example) physicalProps indicating equivalency groups
// A=B=C and ordering D+,A-, the resulting physicalProps has equivalency groups
// 0=2 and ordering 1+,0-.
//
// To support intermingling projected columns with other (e.g. rendered) columns,
// entries in colMap can be -1. For example, for "SELECT A, A+B, C FROM t",
// colMap is {0, -1, 2}. Column 1 will not be part of the ordering or any
// equivalency groups.
func (pp *physicalProps) project(colMap []int) physicalProps {
	var newPP physicalProps

	// For every equivalency group that has at least a column that is projected,
	// pick one such column as a representative for that group.
	//
	// For example, if we have equivalency groups A=B and we are projecting
	// according to "SELECT B, C, D FROM ...", the post-projection columns are
	//  0: B
	//  1: C
	//  2: D
	// so the representative for the equivalency group A=B is column 0.
	//
	// If the projection is "SELECT B, C, A, D FROM ...", the representative is
	// still column 0 (B) and we have an equivalency group between column 0 and
	// column 2.
	newRepr := make(map[int]int)

	for i, c := range colMap {
		if c != -1 {
			group := pp.eqGroups.Find(c)
			if r, ok := newRepr[group]; ok {
				// This group shows up multiple times in the projection.
				newPP.eqGroups.Union(i, r)
			} else {
				// Pick i as a representative for this group.
				newRepr[group] = i
			}
		}
	}

	// Remap constant columns, ignoring column groups that have no projected
	// columns.
	for col, ok := pp.constantCols.Next(0); ok; col, ok = pp.constantCols.Next(col + 1) {
		group := pp.eqGroups.Find(col)
		if r, ok := newRepr[group]; ok {
			newPP.constantCols.Add(newPP.eqGroups.Find(r))
		}
	}

	// Remap not-null columns, ignoring column groups that have no projected
	// columns.
	for col, ok := pp.notNullCols.Next(0); ok; col, ok = pp.notNullCols.Next(col + 1) {
		group := pp.eqGroups.Find(col)
		if r, ok := newRepr[group]; ok {
			newPP.notNullCols.Add(newPP.eqGroups.Find(r))
		}
	}

	// Retain key sets that contain only projected columns.
KeySetLoop:
	for _, k := range pp.weakKeys {
		var newK util.FastIntSet
		for col, ok := k.Next(0); ok; col, ok = k.Next(col + 1) {
			group := pp.eqGroups.Find(col)
			r, ok := newRepr[group]
			if !ok {
				continue KeySetLoop
			}
			newK.Add(r)
		}
		newPP.weakKeys = append(newPP.weakKeys, newK)
	}

	newPP.ordering = make(sqlbase.ColumnOrdering, 0, len(pp.ordering))

	// Preserve the ordering, up to the first column that's not present in the
	// projected columns.
	for _, o := range pp.ordering {
		r, ok := newRepr[o.ColIdx]
		if !ok {
			// None of the columns in the equivalency group are present. We need to
			// break the ordering here.
			// If something is ordered by columns A, then B, then C, if I remove
			// column B I can't say it's ordered by columns A, then C.
			// Example:
			// A | B | C          A | C
			// ---------          -----
			// 1 | 1 | 2   --->   1 | 2
			// 1 | 2 | 1          1 | 1
			// 1 | 2 | 3          1 | 3
			break
		}
		newPP.ordering = append(newPP.ordering, sqlbase.ColumnOrderInfo{
			ColIdx: newPP.eqGroups.Find(r), Direction: o.Direction,
		})
	}
	return newPP
}

// computeMatch computes how long of a prefix of a desired ColumnOrdering is
// matched by the physicalProps.
//
// Returns a value between 0 and len(desired).
func (pp physicalProps) computeMatch(desired sqlbase.ColumnOrdering) int {
	matchLen, _ := pp.computeMatchInternal(desired)
	return matchLen
}

// computeMatchInternal returns both the length of the match and the number of
// columns of pp.ordering necessary for the match.
func (pp physicalProps) computeMatchInternal(
	desired sqlbase.ColumnOrdering,
) (matchLen, ordPos int) {
	pp.check()
	// position in pp.ordering
	pos := 0
	// Set of column groups seen so far.
	var groupsSeen util.FastIntSet

	for i, col := range desired {
		if pp.isKey(groupsSeen) {
			// The columns accumulated so far form a key; any other columns with which
			// we may want to "refine" the ordering don't make a difference.
			return len(desired), pos
		}
		group := pp.eqGroups.Find(col.ColIdx)
		// Check if the column is one of the constant columns.
		if pp.constantCols.Contains(group) {
			continue
		}
		if groupsSeen.Contains(group) {
			// Redundant column; can be ignored.
			continue
		}
		groupsSeen.Add(group)
		if pos < len(pp.ordering) && pp.ordering[pos].ColIdx == group &&
			pp.ordering[pos].Direction == col.Direction {
			// The next column matches.
			pos++
			continue
		}
		// Everything matched up to this point.
		return i, pos
	}
	// Everything matched!
	return len(desired), pos
}

// trim simplifies pp.ordering, retaining only the column groups that are
// needed to to match a desired ordering (or a prefix of it); equivalency
// groups, constant columns, and key sets are left untouched.
//
// A trimmed ordering is guaranteed to still match the desired ordering to the
// same extent, i.e. before and after are equal in:
//   before := pp.computeMatch(desired)
//   pp.trim(desired)
//   after := pp.computeMatch(desired)
func (pp *physicalProps) trim(desired sqlbase.ColumnOrdering) {
	_, pos := pp.computeMatchInternal(desired)
	if pos < len(pp.ordering) {
		pp.ordering = pp.ordering[:pos]
	}
}

// applyExpr tries to extract useful information from an expression we know is
// true on all rows (e.g. a filter expression) and updates the physicalProps
// accordingly. Specifically: it might add constant columns and equivalency
// groups.
func (pp *physicalProps) applyExpr(evalCtx *tree.EvalContext, expr tree.TypedExpr) {
	if expr == nil {
		return
	}
	andExprs := splitAndExpr(evalCtx, expr, nil)
	for _, e := range andExprs {
		// Look for expressions of the form: @x = val or @x = @y.
		if c, ok := e.(*tree.ComparisonExpr); ok && c.Operator == tree.EQ {
			if ok, leftCol := getColVarIdx(c.Left); ok {
				if _, ok := c.Right.(tree.Datum); ok {
					pp.addConstantColumn(leftCol)
				} else if ok, rightCol := getColVarIdx(c.Right); ok {
					pp.addEquivalency(leftCol, rightCol)
				}
			}
		}
		// TODO(radu): look for tuple equalities like (a, b) = (c, d)
	}
	// Infer not-null columns.
	pp.notNullCols.UnionWith(extractNotNullConstraints(expr))
}

// getColVarIdx detects whether an expression is a straightforward
// reference to a column or index variable. In this case it returns
// the index of that column's in the descriptor's []Column array.
func getColVarIdx(expr tree.Expr) (ok bool, colIdx int) {
	switch q := expr.(type) {
	case *tree.IndexedVar:
		return true, q.Idx
	}
	return false, -1
}

// splitAndExpr flattens a tree of AND expressions, appending all of the child
// expressions as a list. Any non-AND expression is appended as a single element
// in the list.
//
//   a AND b AND c AND d -> [a, b, c, d]
func splitAndExpr(
	evalCtx *tree.EvalContext, e tree.TypedExpr, exprs tree.TypedExprs,
) tree.TypedExprs {
	switch t := e.(type) {
	case *tree.AndExpr:
		return splitAndExpr(evalCtx, t.TypedRight(), splitAndExpr(evalCtx, t.TypedLeft(), exprs))
	}
	return append(exprs, e)
}
