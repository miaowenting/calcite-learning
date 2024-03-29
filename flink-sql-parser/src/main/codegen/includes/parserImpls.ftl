<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

/**
* Parse a nullable option, default to be nullable.
*/
boolean NullableOpt() :
{
}
{
    <NULL> { return true; }
|
    <NOT> <NULL> { return false; }
|
    { return true; }
}

/**
 * 获取表字段列
 */
void TableColumn(TableCreationContext context) :
{
}
{
    (LOOKAHEAD(3)
        TableColumn2(context.columnList)
    |
        context.primaryKeyList = PrimaryKey()
    |
        Watermark(context)
    |
        ComputedColumn(context)
    |
        context.sideFlag = SideFlag()
    )
}


void ComputedColumn(TableCreationContext context) :
{
    SqlNode identifier = null;
    SqlNode expr;
    boolean hidden = false;
    SqlParserPos pos;
}
{
            identifier = SimpleIdentifier() {pos = getPos();}
            <AS>
                expr = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                expr = SqlStdOperatorTable.AS.createCall(Span.of(identifier, expr).pos(), expr, identifier);
                context.columnList.add(expr);
                }
}

void Watermark(TableCreationContext context) :
{
    SqlNode identifier;
    SqlNode expr;
    boolean hidden = false;
    SqlParserPos pos;
    SqlIdentifier eventTimeField;
    SqlNode maxOutOrderless;
}
{
    <WATERMARK> <FOR> eventTimeField = SimpleIdentifier() <AS> <withOffset>
    <LPAREN>
         eventTimeField = SimpleIdentifier() { context.eventTimeField = eventTimeField; }
         <COMMA> maxOutOrderless = StringLiteral() {context.maxOutOrderless = maxOutOrderless; }
    <RPAREN>
}


void TableColumn2(List<SqlNode> list) :
{
    SqlParserPos pos;
    SqlIdentifier name;
    SqlDataTypeSpec type;
    SqlIdentifier alias = null;
    SqlCharStringLiteral comment = null;
}
{
    name = SimpleIdentifier()
    <#-- #FlinkDataType already takes care of the nullable attribute. -->
    type = FlinkDataType()
    [ <AS> alias = SimpleIdentifier()
    ]
    [ <COMMENT> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        comment = SqlLiteral.createCharString(p, getPos());
    }]
    {
        SqlTableColumn tableColumn = new SqlTableColumn(name, type , alias ,comment , getPos());
        list.add(tableColumn);
    }
}

/**
 * 主键信息
 */
SqlNodeList PrimaryKey() :
{
    List<SqlNode> pkList = new ArrayList<SqlNode>();

    SqlParserPos pos;
    SqlIdentifier columnName;
}
{
    <PRIMARY> { pos = getPos(); } <KEY> <LPAREN>
        columnName = SimpleIdentifier() { pkList.add(columnName); }
        (<COMMA> columnName = SimpleIdentifier() { pkList.add(columnName); })*
    <RPAREN>
    {
        return new SqlNodeList(pkList, pos.plus(getPos()));
    }
}

/**
 * 唯一键信息
 */
void UniqueKey(List<SqlNodeList> list) :
{
    List<SqlNode> ukList = new ArrayList<SqlNode>();
    SqlParserPos pos;
    SqlIdentifier columnName;
}
{
    <UNIQUE> { pos = getPos(); } <LPAREN>
        columnName = SimpleIdentifier() { ukList.add(columnName); }
        (<COMMA> columnName = SimpleIdentifier() { ukList.add(columnName); })*
    <RPAREN>
    {
        SqlNodeList uk = new SqlNodeList(ukList, pos.plus(getPos()));
        list.add(uk);
    }
}

/**
 * 维表标识
 */
boolean SideFlag() :
{
   SqlParserPos pos;
   SqlIdentifier columnName;
}
{
       <PERIOD> { pos = getPos(); } <FOR> <SYSTEM_TIME> { return true; }
     |
       { return false; }
}


SqlNode TableOption() :
{
    SqlIdentifier key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = CompoundIdentifier()
    { pos = getPos(); }
    <EQ> value = StringLiteral()
    {
        return new SqlTableOption(key, value, getPos());
    }
}

/**
 * Parse a table properties.
 */
SqlNodeList TableProperties():
{
    SqlNode property;
    final List<SqlNode> proList = new ArrayList<SqlNode>();
    final Span span;
}
{
    <LPAREN> { span = span(); }
    [
        property = TableOption()
        {
            proList.add(property);
        }
        (
            <COMMA> property = TableOption()
            {
                proList.add(property);
            }
        )*
    ]
    <RPAREN>
    {  return new SqlNodeList(proList, span.end(this)); }
}

/**
 * 创建 cep 规则
 */
SqlCreate SqlCreateCepRule(Span s,boolean replace) :
{
        final SqlParserPos startPos = s.pos();
        SqlIdentifier cepRuleName;
        SqlIdentifier event = null;
        SqlCharStringLiteral where = null;
        SqlIdentifier repeat = null;
        SqlIdentifier same = null;
        SqlIdentifier windowSize = null;
        SqlIdentifier windowUnit = null;
        SqlCharStringLiteral returnContent = null;
}
{
        <CEPRULE> cepRuleName = CompoundIdentifier()
        [
            <LPAREN>
                <EVENT> event = CompoundIdentifier() <COMMA>
                <WHERE> <QUOTED_STRING>{
                            String p = SqlParserUtil.parseString(token.image);
                            where = SqlLiteral.createCharString(p, getPos());
                        } <COMMA>
                <REPEAT> repeat = CompoundIdentifier() <SAME> same = CompoundIdentifier() <COMMA>
                <WITHIN> {
                            windowSize = CompoundIdentifier();
                            windowUnit = CompoundIdentifier();
                         } <COMMA>
                <RETURN>  <QUOTED_STRING>{
                            String p1 = SqlParserUtil.parseString(token.image);
                            returnContent = SqlLiteral.createCharString(p1, getPos());
                    }
             <RPAREN>
        ]
        {
                    return new SqlCreateCepRule(startPos.plus(getPos()),
                     cepRuleName,
                     event,
                     where,
                     repeat,
                     same,
                     windowSize,
                     windowUnit,
                     returnContent);
        }
}

/**
 * Parse a table creation.
 */
SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SqlParserPos startPos = s.pos();
    SqlIdentifier tableName;
    SqlNodeList primaryKeyList = null;
    SqlNodeList columnList = SqlNodeList.EMPTY;
	SqlCharStringLiteral comment = null;
    boolean sideFlag = false;
    SqlIdentifier eventTimeField = null;
    SqlNode maxOutOrderless = null;

    SqlNodeList propertyList = SqlNodeList.EMPTY;
    SqlParserPos pos = startPos;
}
{
    <TABLE>

    tableName = CompoundIdentifier()
    [
        <LPAREN> { pos = getPos(); TableCreationContext ctx = new TableCreationContext();}
        TableColumn(ctx)
        (
            <COMMA> TableColumn(ctx)
        )*
        {
            pos = pos.plus(getPos());
            columnList = new SqlNodeList(ctx.columnList, pos);
            primaryKeyList = ctx.primaryKeyList;
            sideFlag = ctx.sideFlag;
            eventTimeField = ctx.eventTimeField;
            maxOutOrderless = ctx.maxOutOrderless;
        }
        <RPAREN>
    ]

    [ <COMMENT> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        comment = SqlLiteral.createCharString(p, getPos());
    }]
    [
        <WITH>
        propertyList = TableProperties()
    ]
    {
        return new SqlCreateTable(startPos.plus(getPos()),
                tableName,
                columnList,
                primaryKeyList,
                propertyList,
                comment,
                sideFlag,
                eventTimeField,
                maxOutOrderless);
    }
}

/**
 * 创建函数
 */
SqlCreateFunction SqlCreateFunction(Span s, boolean replace):
{
    SqlParserPos pos;
    SqlIdentifier functionName;
    SqlNode className;
}
{
    { pos = getPos(); }
    <FUNCTION> functionName = CompoundIdentifier()
    <AS> className = StringLiteral()
    {
        return new SqlCreateFunction(s.pos(),functionName,className);
    }
}



/**
* Parses an INSERT statement.
*/
SqlNode RichSqlInsert() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    final List<SqlLiteral> extendedKeywords = new ArrayList<SqlLiteral>();
    final SqlNodeList extendedKeywordList;
    SqlNode table;
    SqlNodeList extendList = null;
    SqlNode source;
    final SqlNodeList partitionList = new SqlNodeList(getPos());
    SqlNodeList columnList = null;
    final Span s;
}
{
    (
        <INSERT>
    |
        <UPSERT> { keywords.add(SqlInsertKeyword.UPSERT.symbol(getPos())); }
    )
    (
        <INTO>
    |
        <OVERWRITE> {
            if (!((FlinkSqlConformance) this.conformance).allowInsertOverwrite()) {
                throw new ParseException("OVERWRITE expression is only allowed for HIVE dialect");
            } else if (RichSqlInsert.isUpsert(keywords)) {
                throw new ParseException("OVERWRITE expression is only used with INSERT mode");
            }
            extendedKeywords.add(RichSqlInsertKeyword.OVERWRITE.symbol(getPos()));
        }
    )
    { s = span(); }
    SqlInsertKeywords(keywords) {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
        extendedKeywordList = new SqlNodeList(extendedKeywords, s.addAll(extendedKeywords).pos());
    }
    table = CompoundIdentifier()
    [
        LOOKAHEAD(5)
        [ <EXTEND> ]
        extendList = ExtendList() {
            table = extend(table, extendList);
        }
    ]
    [
        LOOKAHEAD(2)
        { final Pair<SqlNodeList, SqlNodeList> p; }
        p = ParenthesizedCompoundIdentifierList() {
            if (p.right.size() > 0) {
                table = extend(table, p.right);
            }
            if (p.left.size() > 0) {
                columnList = p.left;
            }
        }
    ]
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new RichSqlInsert(s.end(source), keywordList, extendedKeywordList, table, source,
            columnList, partitionList);
    }
}

/**
* Parses a partition specifications statement,
* e.g. insert into tbl1 partition(col1='val1', col2='val2') select col3 from tbl.
*/
<#--void PartitionSpecCommaList(SqlNodeList list) :-->
<#--{-->
<#--    SqlIdentifier key;-->
<#--    SqlNode value;-->
<#--    SqlParserPos pos;-->
<#--}-->
<#--{-->
<#--    <LPAREN>-->
<#--    key = SimpleIdentifier()-->
<#--    { pos = getPos(); }-->
<#--    <EQ> value = Literal() {-->
<#--        list.add(new SqlProperty(key, value, pos));-->
<#--    }-->
<#--    (-->
<#--        <COMMA> key = SimpleIdentifier() { pos = getPos(); }-->
<#--        <EQ> value = Literal() {-->
<#--            list.add(new SqlProperty(key, value, pos));-->
<#--        }-->
<#--    )*-->
<#--    <RPAREN>-->
<#--}-->

/**
* Parses a create view or replace existing view statement.
*   CREATE [OR REPLACE] VIEW view_name [ (field1, field2 ...) ] AS select_statement
*/
SqlCreate SqlCreateView(Span s, boolean replace) : {
    SqlIdentifier viewName;
    SqlCharStringLiteral comment = null;
    SqlNode query;
    SqlNodeList fieldList = SqlNodeList.EMPTY;
}
{
    <VIEW>
    viewName = CompoundIdentifier()
    [
        fieldList = ParenthesizedSimpleIdentifierList()
    ]
    [ <COMMENT> <QUOTED_STRING> {
            String p = SqlParserUtil.parseString(token.image);
            comment = SqlLiteral.createCharString(p, getPos());
        }
    ]
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateView(s.pos(), viewName, fieldList, query, replace, comment);
    }
}

SqlIdentifier FlinkCollectionsTypeName() :
{
}
{
    LOOKAHEAD(2)
    <MULTISET> {
        return new SqlIdentifier(SqlTypeName.MULTISET.name(), getPos());
    }
|
    <ARRAY> {
        return new SqlIdentifier(SqlTypeName.ARRAY.name(), getPos());
    }
}

SqlIdentifier FlinkTypeName() :
{
    final SqlTypeName sqlTypeName;
    final SqlIdentifier typeName;
    final Span s = Span.of();
}
{
    (
<#-- additional types are included here -->
<#-- make custom data types in front of Calcite core data types -->
<#list parser.flinkDataTypeParserMethods as method>
    <#if (method?index > 0)>
    |
    </#if>
        LOOKAHEAD(2)
        typeName = ${method}
</#list>
    |
        LOOKAHEAD(2)
        sqlTypeName = SqlTypeName(s) {
            typeName = new SqlIdentifier(sqlTypeName.name(), s.end(this));
        }
    |
        LOOKAHEAD(2)
        typeName = FlinkCollectionsTypeName()
    |
        typeName = CompoundIdentifier() {
            throw new ParseException("UDT in DDL is not supported yet.");
        }
    )
    {
        return typeName;
    }
}

/**
* Parse a Flink data type with nullable options, NULL -> nullable, NOT NULL -> not nullable.
* Default to be nullable.
*/
SqlDataTypeSpec FlinkDataType() :
{
    final SqlIdentifier typeName;
    SqlIdentifier collectionTypeName = null;
    int scale = -1;
    int precision = -1;
    String charSetName = null;
    final Span s;
    boolean nullable = true;
    boolean elementNullable = true;
}
{
    typeName = FlinkTypeName() {
        s = span();
    }
    [
        <LPAREN>
        precision = UnsignedIntLiteral()
        [
            <COMMA>
            scale = UnsignedIntLiteral()
        ]
        <RPAREN>
    ]
    elementNullable = NullableOpt()
    [
        collectionTypeName = FlinkCollectionsTypeName()
        nullable = NullableOpt()
    ]
    {
        if (null != collectionTypeName) {
            return new FlinkSqlDataTypeSpec(
                    collectionTypeName,
                    typeName,
                    precision,
                    scale,
                    charSetName,
                    nullable,
                    elementNullable,
                    s.end(collectionTypeName));
        }
        nullable = elementNullable;
        return new FlinkSqlDataTypeSpec(typeName,
                precision,
                scale,
                charSetName,
                null,
                nullable,
                elementNullable,
                s.end(this));
    }
}

SqlIdentifier SqlStringType() :
{
}
{
    <STRING> { return new SqlStringType(getPos()); }
}

SqlIdentifier SqlBytesType() :
{
}
{
    <BYTES> { return new SqlBytesType(getPos()); }
}

boolean WithLocalTimeZone() :
{
}
{
    <WITHOUT> <TIME> <ZONE> { return false; }
|
    <WITH>
    (
         <LOCAL> <TIME> <ZONE> { return true; }
    |
        <TIME> <ZONE> {
            throw new ParseException("'WITH TIME ZONE' is not supported yet, options: " +
                "'WITHOUT TIME ZONE', 'WITH LOCAL TIME ZONE'.");
        }
    )
|
    { return false; }
}

SqlIdentifier SqlTimeType() :
{
    int precision = -1;
    boolean withLocalTimeZone = false;
}
{
    <TIME>
    (
        <LPAREN> precision = UnsignedIntLiteral() <RPAREN>
    |
        { precision = -1; }
    )
    withLocalTimeZone = WithLocalTimeZone()
    { return new SqlTimeType(getPos(), precision, withLocalTimeZone); }
}

SqlIdentifier SqlTimestampType() :
{
    int precision = -1;
    boolean withLocalTimeZone = false;
}
{
    <TIMESTAMP>
    (
        <LPAREN> precision = UnsignedIntLiteral() <RPAREN>
    |
        { precision = -1; }
    )
    withLocalTimeZone = WithLocalTimeZone()
    { return new SqlTimestampType(getPos(), precision, withLocalTimeZone); }
}

SqlIdentifier SqlArrayType() :
{
    SqlParserPos pos;
    SqlDataTypeSpec elementType;
    boolean nullable = true;
}
{
    <ARRAY> { pos = getPos(); }
    <LT>
    elementType = FlinkDataType()
    <GT>
    {
        return new SqlArrayType(pos, elementType);
    }
}

SqlIdentifier SqlMultisetType() :
{
    SqlParserPos pos;
    SqlDataTypeSpec elementType;
    boolean nullable = true;
}
{
    <MULTISET> { pos = getPos(); }
    <LT>
    elementType = FlinkDataType()
    <GT>
    {
        return new SqlMultisetType(pos, elementType);
    }
}

SqlIdentifier SqlMapType() :
{
    SqlDataTypeSpec keyType;
    SqlDataTypeSpec valType;
    boolean nullable = true;
}
{
    <MAP>
    <LT>
    keyType = FlinkDataType()
    <COMMA>
    valType = FlinkDataType()
    <GT>
    {
        return new SqlMapType(getPos(), keyType, valType);
    }
}

/**
* Parse a "name1 type1 ['i'm a comment'], name2 type2 ..." list.
*/
void FieldNameTypeCommaList(
        List<SqlIdentifier> fieldNames,
        List<SqlDataTypeSpec> fieldTypes,
        List<SqlCharStringLiteral> comments) :
{
    SqlIdentifier fName;
    SqlDataTypeSpec fType;
}
{
    [
        fName = SimpleIdentifier()
        fType = FlinkDataType()
        {
            fieldNames.add(fName);
            fieldTypes.add(fType);
        }
        (
            <QUOTED_STRING> {
                String p = SqlParserUtil.parseString(token.image);
                comments.add(SqlLiteral.createCharString(p, getPos()));
            }
        |
            { comments.add(null); }
        )
    ]
    (
        <COMMA>
        fName = SimpleIdentifier()
        fType = FlinkDataType()
        {
            fieldNames.add(fName);
            fieldTypes.add(fType);
        }
        (
            <QUOTED_STRING> {
                String p = SqlParserUtil.parseString(token.image);
                comments.add(SqlLiteral.createCharString(p, getPos()));
            }
        |
            { comments.add(null); }
        )
    )*
}

/**
* Parse Row type, we support both Row(name1 type1, name2 type2) and Row<name1 type1, name2 type2>.
* Every item type can have suffix of `NULL` or `NOT NULL` to indicate if this type is nullable.
* i.e. Row(f0 int not null, f1 varchar null).
*/
SqlIdentifier SqlRowType() :
{
    List<SqlIdentifier> fieldNames = new ArrayList<SqlIdentifier>();
    List<SqlDataTypeSpec> fieldTypes = new ArrayList<SqlDataTypeSpec>();
    List<SqlCharStringLiteral> comments = new ArrayList<SqlCharStringLiteral>();
}
{
    <ROW>
    (
        <NE>
    |
        <LT> FieldNameTypeCommaList(fieldNames, fieldTypes, comments) <GT>
    |
        <LPAREN> FieldNameTypeCommaList(fieldNames, fieldTypes, comments) <RPAREN>
    )
    {
        return new SqlRowType(getPos(), fieldNames, fieldTypes, comments);
    }
}

/**
 * DESCRIBE | DESC [ EXTENDED] [[catalogName.] dataBasesName].tableName sql call.
 * Here we add Rich in className to distinguish from calcite's original SqlDescribeTable.
 */
SqlRichDescribeTable SqlRichDescribeTable() :
{
    SqlIdentifier tableName;
    SqlParserPos pos;
    boolean isExtended = false;
}
{
    ( <DESCRIBE> | <DESC> ) { pos = getPos();}
    [ <EXTENDED> { isExtended = true;} ]
    tableName = CompoundIdentifier()
    {
        return new SqlRichDescribeTable(pos, tableName, isExtended);
    }
}

/**
* Parses an explain module statement.
*/
SqlNode SqlRichExplain() :
{
    SqlNode stmt;
    Set<String> explainDetails = new HashSet<String>();
}
{
    (
    LOOKAHEAD(3) <EXPLAIN> <PLAN> <FOR>
    |
    LOOKAHEAD(2) <EXPLAIN> ParseExplainDetail(explainDetails) ( <COMMA> ParseExplainDetail(explainDetails) )*
    |
    <EXPLAIN>
    )
    (
        stmt = SqlStatementSet()
        |
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        |
        stmt = RichSqlInsert()
    )
    {
        return new SqlRichExplain(getPos(), stmt, explainDetails);
    }
}

/**
* Parse a statement set.
*
* STATEMENT SET BEGIN (RichSqlInsert();)+ END
*/
SqlNode SqlStatementSet() :
{
    SqlParserPos startPos;
    SqlNode insert;
    List<RichSqlInsert> inserts = new ArrayList<RichSqlInsert>();
}
{
    <STATEMENT>{ startPos = getPos(); } <SET> <BEGIN>
    (
        insert = RichSqlInsert()
        <SEMICOLON>
        {
            inserts.add((RichSqlInsert) insert);
        }
    )+
    <END>
    {
        return new SqlStatementSet(inserts, startPos);
    }
}

void ParseExplainDetail(Set<String> explainDetails):
{
}
{
    (
        <ESTIMATED_COST>
        |
        <CHANGELOG_MODE>
        |
        <JSON_EXECUTION_PLAN>
    )
    {
        if (explainDetails.contains(token.image.toUpperCase())) {
            throw SqlUtil.newContextException(
                getPos(),
                ParserResource.RESOURCE.explainDetailIsDuplicate());
        } else {
            explainDetails.add(token.image.toUpperCase());
        }
    }
}

/**
* SHOW TABLES FROM [catalog.] database sql call.
*/
SqlShowTables SqlShowTables() :
{
    SqlIdentifier databaseName = null;
    SqlCharStringLiteral likeLiteral = null;
    String prep = null;
    boolean notLike = false;
    SqlParserPos pos;
}
{
    <SHOW> <TABLES>
    { pos = getPos(); }
    [
        ( <FROM> { prep = "FROM"; } | <IN> { prep = "IN"; } )
        { pos = getPos(); }
        databaseName = CompoundIdentifier()
    ]
    [
        [
            <NOT>
            {
                notLike = true;
            }
        ]
        <LIKE>  <QUOTED_STRING>
        {
            String likeCondition = SqlParserUtil.parseString(token.image);
            likeLiteral = SqlLiteral.createCharString(likeCondition, getPos());
        }
    ]
    {
        return new SqlShowTables(pos, prep, databaseName, notLike, likeLiteral);
    }
}