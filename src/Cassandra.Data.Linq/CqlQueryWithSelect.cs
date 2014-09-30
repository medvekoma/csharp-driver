//
//      Copyright (C) 2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Linq;
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    using System.Collections.Generic;

    public class CqlQueryWithSelect<TSource, TResult> : CqlQuery<TResult>
    {
        private readonly Func<TSource, TResult> _selectorFunc;

        internal CqlQueryWithSelect(Expression expression, IQueryProvider table, Expression<Func<TSource, TResult>> selector)
            : base(expression, table)
        {
            _selectorFunc = selector.Compile();
        }

        internal override IEnumerable<TResult> AdaptRows(IEnumerable<Row> rows, Dictionary<string, int> colToIdx, CqlExpressionVisitor visitor)
        {
            return rows.Select(row =>
                CqlQueryTools.GetRowFromCqlRow<TSource>(row, colToIdx, visitor.Mappings, visitor.Alter))
                .Select(source => _selectorFunc(source));
        }
    }
}