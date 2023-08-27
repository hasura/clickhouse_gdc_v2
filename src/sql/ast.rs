use std::fmt;

pub struct Statement(pub Query);

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} FORMAT JSON;", self.0)
    }
}

pub struct Query {
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
    predicate: Option<Expr>,
    group_by: Vec<Expr>,
    order_by: Vec<OrderByExpr>,
    limit_by: Option<LimitByExpr>,
    limit: Option<u64>,
    offset: Option<u64>,
}

impl Query {
    pub fn new(projection: Vec<SelectItem>) -> Self {
        Self {
            projection,
            from: vec![],
            predicate: None,
            group_by: vec![],
            order_by: vec![],
            limit_by: None,
            limit: None,
            offset: None,
        }
    }
    pub fn from(self, from: Vec<TableWithJoins>) -> Self {
        Self { from, ..self }
    }
    pub fn predicate(self, predicate: Option<Expr>) -> Self {
        Self { predicate, ..self }
    }
    pub fn group_by(self, group_by: Vec<Expr>) -> Self {
        Self { group_by, ..self }
    }
    pub fn order_by(self, order_by: Vec<OrderByExpr>) -> Self {
        Self { order_by, ..self }
    }
    pub fn limit_by(self, limit_by: Option<LimitByExpr>) -> Self {
        Self { limit_by, ..self }
    }
    pub fn limit(self, limit: Option<u64>) -> Self {
        Self { limit, ..self }
    }
    pub fn offset(self, offset: Option<u64>) -> Self {
        Self { offset, ..self }
    }
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT {}", display_separated(&self.projection, ", "))?;
        if !self.from.is_empty() {
            write!(f, " FROM {}", display_separated(&self.from, ", "))?;
        }
        if let Some(predicate) = &self.predicate {
            write!(f, " WHERE {}", predicate)?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY {}", display_separated(&self.group_by, ", "))?;
        }
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY {}", display_separated(&self.order_by, ", "))?;
        }
        if let Some(limit_by) = &self.limit_by {
            write!(f, " LIMIT {}", limit_by.limit)?;
            if let Some(offset) = limit_by.offset {
                write!(f, " OFFSET {}", offset)?;
            }
            write!(f, " BY {}", display_separated(&limit_by.by, ", "))?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " LIMIT {}", limit)?;
        }
        if let Some(offset) = &self.offset {
            write!(f, " OFFSET {}", offset)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LimitByExpr {
    pub limit: u64,
    pub offset: Option<u64>,
    pub by: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: Option<bool>,
    pub nulls_first: Option<bool>,
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        match self.asc {
            Some(true) => write!(f, " ASC")?,
            Some(false) => write!(f, " DESC")?,
            None => (),
        }
        match self.nulls_first {
            Some(true) => write!(f, " NULLS FIRST")?,
            Some(false) => write!(f, " NULLS LAST")?,
            None => (),
        }
        Ok(())
    }
}

pub enum SelectItem {
    UnnamedExpr(Expr),
    ExprWithAlias { expr: Expr, alias: Ident },
    QualifiedWildcard(ObjectName),
    Wildcard,
}

impl fmt::Display for SelectItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectItem::UnnamedExpr(expr) => write!(f, "{}", expr),
            SelectItem::ExprWithAlias { expr, alias } => write!(f, "{} AS {}", expr, alias),
            SelectItem::QualifiedWildcard(name) => write!(f, "{}.*", name),
            SelectItem::Wildcard => write!(f, "*"),
        }
    }
}

pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

impl fmt::Display for TableWithJoins {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.relation)?;
        for join in &self.joins {
            write!(f, " {}", join)?;
        }
        Ok(())
    }
}

pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn prefix(constraint: &JoinConstraint) -> &'static str {
            match constraint {
                JoinConstraint::Natural => "NATURAL ",
                _ => "",
            }
        }
        fn suffix(constraint: &'_ JoinConstraint) -> impl fmt::Display + '_ {
            struct Suffix<'a>(&'a JoinConstraint);
            impl<'a> fmt::Display for Suffix<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        JoinConstraint::On(expr) => write!(f, " ON {expr}"),
                        JoinConstraint::Using(attrs) => {
                            write!(f, " USING({})", display_separated(attrs, ", "))
                        }
                        _ => Ok(()),
                    }
                }
            }
            Suffix(constraint)
        }
        match &self.join_operator {
            JoinOperator::Inner(constraint) => write!(
                f,
                " {}JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::LeftOuter(constraint) => write!(
                f,
                " {}LEFT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::RightOuter(constraint) => write!(
                f,
                " {}RIGHT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::FullOuter(constraint) => write!(
                f,
                " {}FULL JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::CrossJoin => write!(f, " CROSS JOIN {}", self.relation),
        }
    }
}

pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin,
}

pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Ident>),
    Natural,
    None,
}

pub enum TableFactor {
    Table {
        name: ObjectName,
        alias: Option<Ident>,
    },
    Derived {
        subquery: Box<Query>,
        alias: Option<Ident>,
    },
    TableFunction {
        function: Function,
        alias: Option<Ident>,
    },
}

impl fmt::Display for TableFactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableFactor::Table { name, alias } => {
                write!(f, "{}", name)?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias)?;
                }
            }
            TableFactor::Derived { subquery, alias } => {
                write!(f, "({})", subquery)?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias)?;
                }
            }
            TableFactor::TableFunction { function, alias } => {
                write!(f, "{}", function)?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", alias)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ObjectName(pub Vec<Ident>);

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", display_separated(&self.0, "."))
    }
}

#[derive(Debug, Clone)]
pub enum Expr {
    Identifier(Ident),
    CompoundIdentifier(Vec<Ident>),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Nested(Box<Expr>),
    Value(Value),
    Function(Function),
    IsFalse(Box<Expr>),
    IsNotFalse(Box<Expr>),
    IsTrue(Box<Expr>),
    IsNotTrue(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    NotInList {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Identifier(ident) => write!(f, "{}", ident),
            Expr::CompoundIdentifier(idents) => write!(f, "{}", display_separated(idents, ".")),
            Expr::BinaryOp { left, op, right } => write!(f, "{} {} {}", left, op, right),
            Expr::UnaryOp { op, expr } => write!(f, "{} {}", op, expr),
            Expr::Nested(expr) => write!(f, "({})", expr),
            Expr::Value(value) => write!(f, "{}", value),
            Expr::Function(function) => write!(f, "{}", function),
            Expr::IsTrue(expr) => write!(f, "{expr} IS TRUE"),
            Expr::IsNotTrue(expr) => write!(f, "{expr} IS NOT TRUE"),
            Expr::IsFalse(expr) => write!(f, "{expr} IS FALSE"),
            Expr::IsNotFalse(expr) => write!(f, "{expr} IS NOT FALSE"),
            Expr::IsNull(expr) => write!(f, "{expr} IS NULL"),
            Expr::IsNotNull(expr) => write!(f, "{expr} IS NOT NULL"),
            Expr::InList { expr, list } => {
                write!(f, "{} IN ({})", expr, display_separated(list, ", "),)
            }
            Expr::NotInList { expr, list } => {
                write!(f, "{} NOT IN ({})", expr, display_separated(list, ", "),)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Function {
    pub name: ObjectName,
    pub args: Vec<FunctionArgExpr>,
    pub over: Option<WindowSpec>,
    pub distinct: bool,
}

impl fmt::Display for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}{})",
            self.name,
            if self.distinct { "DISTINCT " } else { "" },
            display_separated(&self.args, ", ")
        )?;
        if let Some(over) = &self.over {
            write!(f, " OVER ({})", over)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
}

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.partition_by.is_empty() {
            write!(
                f,
                "PARTITION BY {}",
                display_separated(&self.partition_by, ", ")
            )?;
        }
        if !self.order_by.is_empty() {
            if !self.partition_by.is_empty() {
                write!(f, " ")?;
            }
            write!(f, "ORDER BY {}", display_separated(&self.order_by, ", "))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum FunctionArgExpr {
    Expr(Expr),
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    QualifiedWildcard(ObjectName),
    /// An unqualified `*`
    Wildcard,
}

impl fmt::Display for FunctionArgExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionArgExpr::Expr(expr) => write!(f, "{}", expr),
            FunctionArgExpr::QualifiedWildcard(name) => write!(f, "{}.*", name),
            FunctionArgExpr::Wildcard => write!(f, "*"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum UnaryOperator {
    Not,
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOperator::Not => write!(f, "NOT"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum BinaryOperator {
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOperator::Gt => write!(f, ">"),
            BinaryOperator::Lt => write!(f, "<"),
            BinaryOperator::GtEq => write!(f, ">="),
            BinaryOperator::LtEq => write!(f, "<="),
            BinaryOperator::Eq => write!(f, "="),
            BinaryOperator::NotEq => write!(f, "!="),
            BinaryOperator::And => write!(f, "AND"),
            BinaryOperator::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Number(String),
    SingleQuotedString(String),
    Boolean(bool),
    Null,
    Placeholder(String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Number(n) => write!(f, "{}", n),
            Value::SingleQuotedString(s) => {
                // docs: https://clickhouse.com/docs/en/sql-reference/syntax#syntax-string-literal
                let escaped_value = s.to_owned().replace('\\', r#"\\"#).replace('\'', r#"\'"#);
                write!(f, "'{}'", escaped_value)
            }
            Value::Boolean(b) => {
                if *b {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            Value::Null => write!(f, "NULL"),
            Value::Placeholder(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Ident {
    value: String,
    quoted: bool,
}

impl Ident {
    pub fn new<S: Into<String>>(value: S, quoted: bool) -> Self {
        Self {
            value: value.into(),
            quoted,
        }
    }

    pub fn quoted<S: Into<String>>(value: S) -> Self {
        Self {
            value: value.into(),
            quoted: true,
        }
    }
    pub fn unquoted<S: Into<String>>(value: S) -> Self {
        Self {
            value: value.into(),
            quoted: false,
        }
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.quoted {
            write!(f, "\"{}\"", self.value)
        } else {
            write!(f, "{}", self.value)
        }
    }
}

pub struct DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    slice: &'a [T],
    separator: &'static str,
}

fn display_separated<'a, T>(slice: &'a [T], separator: &'static str) -> DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, separator }
}

impl<'a, T> fmt::Display for DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut first = true;
        for t in self.slice {
            if first {
                first = false;
            } else {
                write!(f, "{}", self.separator)?;
            }
            write!(f, "{}", t)?;
        }
        Ok(())
    }
}
